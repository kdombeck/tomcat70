/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.catalina.session;

import org.apache.catalina.Container;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.util.CustomObjectInputStream;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Implementation of the <code>Store</code> interface that stores
 * serialized session objects in a database using a specified data source.  Sessions that are
 * saved are still subject to being expired based on inactivity.
 * Note that this implementation does not synchronize access to a connection.
 *
 * @author Viktor Khoroshko
 * @version $Id$
 */
public class DataSourceStore extends StoreBase implements Store {
    /**
     * The descriptive information about this implementation.
     */
    protected static String info = "DataSourceStore/1.0";

    /**
     * Context name associated with this Store
     */
    private String name = null;

    /**
     * Name to register for this Store, used for logging.
     */
    protected static String storeName = "DataSourceStore";

    /**
     * Name to register for the background thread.
     */
    protected String threadName = "DataSourceStore";

    /**
     * The name of the JNDI JDBC DataSource
     */
    protected String dataSourceName = null;

    /**
     * * DataSource to use
     */
    protected DataSource dataSource = null;

    // ------------------------------------------------------------- Table & cols

    /**
     * Table to use.
     */
    protected String sessionTable = "tomcat$sessions";

    /**
     * Column to use for /Engine/Host/Context name
     */
    protected String sessionAppCol = "app";

    /**
     * Id column to use.
     */
    protected String sessionIdCol = "id";

    /**
     * Data column to use.
     */
    protected String sessionDataCol = "data";

    /**
     * Is Valid column to use.
     */
    protected String sessionValidCol = "valid";

    /**
     * Max Inactive column to use.
     */
    protected String sessionMaxInactiveCol = "maxinactive";

    /**
     * Last Accessed column to use.
     */
    protected String sessionLastAccessedCol = "lastaccess";

    // ------------------------------------------------------------- SQL Variables

    /**
     * The generated string for the size PreparedStatement
     */
    protected String preparedSizeSql = null;

    /**
     * The generated string for the keys PreparedStatement
     */
    protected String preparedKeysSql = null;

    /**
     * The generated string for the save PreparedStatement
     */
    protected String preparedSaveSql = null;

    /**
     * The generated string for the clear PreparedStatement
     */
    protected String preparedClearSql = null;

    /**
     * The generated string for the removes PreparedStatement
     */
    protected String preparedRemoveSql = null;

    /**
     * The generated string for the load PreparedStatement
     */
    protected String preparedLoadSql = null;

    // ------------------------------------------------------------- Properties

    /**
     * Return the info for this Store.
     */
    @Override
    public String getInfo() {
        return (info);
    }

    /**
     * Return the name for this instance (built from container name)
     */
    public String getName() {
        if (name == null) {
            Container container = manager.getContainer();
            String contextName = container.getName();
            if (!contextName.startsWith("/")) {
                contextName = "/" + contextName;
            }
            String hostName = "";
            String engineName = "";

            if (container.getParent() != null) {
                Container host = container.getParent();
                hostName = host.getName();
                if (host.getParent() != null) {
                    engineName = host.getParent().getName();
                }
            }
            name = "/" + engineName + "/" + hostName + contextName;
        }
        return name;
    }

    /**
     * Return the thread name for this Store.
     */
    public String getThreadName() {
        return (threadName);
    }

    /**
     * Return the name for this Store, used for logging.
     */
    @Override
    public String getStoreName() {
        return (storeName);
    }

    /**
     * Set the table for this Store.
     *
     * @param sessionTable The new table
     */
    public void setSessionTable(String sessionTable) {
        String oldSessionTable = this.sessionTable;
        this.sessionTable = sessionTable;
        support.firePropertyChange("sessionTable",
                oldSessionTable,
                this.sessionTable);
    }

    /**
     * Return the table for this Store.
     */
    public String getSessionTable() {
        return (this.sessionTable);
    }

    /**
     * Set the App column for the table.
     *
     * @param sessionAppCol the column name
     */
    public void setSessionAppCol(String sessionAppCol) {
        String oldSessionAppCol = this.sessionAppCol;
        this.sessionAppCol = sessionAppCol;
        support.firePropertyChange("sessionAppCol",
                oldSessionAppCol,
                this.sessionAppCol);
    }

    /**
     * Return the web application name column for the table.
     */
    public String getSessionAppCol() {
        return (this.sessionAppCol);
    }

    /**
     * Set the Id column for the table.
     *
     * @param sessionIdCol the column name
     */
    public void setSessionIdCol(String sessionIdCol) {
        String oldSessionIdCol = this.sessionIdCol;
        this.sessionIdCol = sessionIdCol;
        support.firePropertyChange("sessionIdCol",
                oldSessionIdCol,
                this.sessionIdCol);
    }

    /**
     * Return the Id column for the table.
     */
    public String getSessionIdCol() {
        return (this.sessionIdCol);
    }

    /**
     * Set the Data column for the table
     *
     * @param sessionDataCol the column name
     */
    public void setSessionDataCol(String sessionDataCol) {
        String oldSessionDataCol = this.sessionDataCol;
        this.sessionDataCol = sessionDataCol;
        support.firePropertyChange("sessionDataCol",
                oldSessionDataCol,
                this.sessionDataCol);
    }

    /**
     * Return the data column for the table
     */
    public String getSessionDataCol() {
        return (this.sessionDataCol);
    }

    /**
     * Set the Is Valid column for the table
     *
     * @param sessionValidCol The column name
     */
    public void setSessionValidCol(String sessionValidCol) {
        String oldSessionValidCol = this.sessionValidCol;
        this.sessionValidCol = sessionValidCol;
        support.firePropertyChange("sessionValidCol",
                oldSessionValidCol,
                this.sessionValidCol);
    }

    /**
     * Return the Is Valid column
     */
    public String getSessionValidCol() {
        return (this.sessionValidCol);
    }

    /**
     * Set the Max Inactive column for the table
     *
     * @param sessionMaxInactiveCol The column name
     */
    public void setSessionMaxInactiveCol(String sessionMaxInactiveCol) {
        String oldSessionMaxInactiveCol = this.sessionMaxInactiveCol;
        this.sessionMaxInactiveCol = sessionMaxInactiveCol;
        support.firePropertyChange("sessionMaxInactiveCol",
                oldSessionMaxInactiveCol,
                this.sessionMaxInactiveCol);
    }

    /**
     * Return the Max Inactive column
     */
    public String getSessionMaxInactiveCol() {
        return (this.sessionMaxInactiveCol);
    }

    /**
     * Set the Last Accessed column for the table
     *
     * @param sessionLastAccessedCol The column name
     */
    public void setSessionLastAccessedCol(String sessionLastAccessedCol) {
        String oldSessionLastAccessedCol = this.sessionLastAccessedCol;
        this.sessionLastAccessedCol = sessionLastAccessedCol;
        support.firePropertyChange("sessionLastAccessedCol",
                oldSessionLastAccessedCol,
                this.sessionLastAccessedCol);
    }

    /**
     * Return the Last Accessed column
     */
    public String getSessionLastAccessedCol() {
        return (this.sessionLastAccessedCol);
    }

    /**
     * Set the JNDI name of a DataSource-factory to use for db access
     *
     * @param dataSourceName The JNDI name of the DataSource-factory
     */
    public void setDataSourceName(String dataSourceName) {
        if (dataSourceName == null || "".equals(dataSourceName.trim())) {
            manager.getContainer().getLogger().warn(
                    sm.getString(getStoreName() + ".missingDataSourceName"));
            return;
        }
        this.dataSourceName = dataSourceName;
    }

    /**
     * Return the name of the JNDI DataSource-factory
     */
    public String getDataSourceName() {
        return this.dataSourceName;
    }

    // --------------------------------------------------------- Public Methods

    /**
     * Return an array containing the session identifiers of all Sessions
     * currently saved in this Store.  If there are no such Sessions, a
     * zero-length array is returned.
     */
    @Override
    public String[] keys() {
        ResultSet rst = null;
        String keys[] = null;
        PreparedStatement preparedKeys = null;

        Connection _conn = getConnection();
        if (_conn == null) {
            return (new String[0]);
        }
        try {
            preparedKeys = _conn.prepareStatement(preparedKeysSql);
            preparedKeys.setString(1, getName());
            rst = preparedKeys.executeQuery();
            ArrayList<String> tmpkeys = new ArrayList<String>();
            if (rst != null) {
                while (rst.next()) {
                    tmpkeys.add(rst.getString(1));
                }
            }
            keys = tmpkeys.toArray(new String[tmpkeys.size()]);
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            keys = new String[0];
        } finally {
            try {
                if (preparedKeys != null) {
                    preparedKeys.close();
                }
                if (rst != null) {
                    rst.close();
                }
            } catch (SQLException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            }

            close(_conn);
        }

        return (keys);
    }

    /**
     * Return an integer containing a count of all Sessions
     * currently saved in this Store.  If there are no Sessions,
     * <code>0</code> is returned.
     */
    @Override
    public int getSize() {
        int size = 0;
        ResultSet rst = null;
        PreparedStatement preparedSize = null;

        Connection _conn = getConnection();
        if (_conn == null) {
            return (size);
        }

        try {
            preparedSize = _conn.prepareStatement(preparedSizeSql);
            preparedSize.setString(1, getName());
            rst = preparedSize.executeQuery();
            if (rst.next()) {
                size = rst.getInt(1);
            }
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
        } finally {
            try {
                if (preparedSize != null) {
                    preparedSize.close();
                }
                if (rst != null) {
                    rst.close();
                }
            } catch (SQLException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            }

            close(_conn);
        }

        return (size);
    }

    /**
     * Load the Session associated with the id <code>id</code>.
     * If no such session is found <code>null</code> is returned.
     *
     * @param id a value of type <code>String</code>
     * @return the stored <code>Session</code>
     * @throws ClassNotFoundException if an error occurs
     * @throws IOException            if an input/output error occurred
     */
    @Override
    public Session load(String id)
            throws ClassNotFoundException, IOException {
        ResultSet rst = null;
        PreparedStatement preparedLoad = null;
        StandardSession _session = null;
        Loader loader = null;
        ClassLoader classLoader = null;
        ObjectInputStream ois = null;
        BufferedInputStream bis = null;
        Container container = manager.getContainer();

        Connection _conn = getConnection();
        if (_conn == null) {
            return (null);
        }

        try {
            preparedLoad = _conn.prepareStatement(preparedLoadSql);
            preparedLoad.setString(1, id);
            preparedLoad.setString(2, getName());
            rst = preparedLoad.executeQuery();
            if (rst.next()) {
                bis = new BufferedInputStream(rst.getBinaryStream(2));

                if (container != null) {
                    loader = container.getLoader();
                }
                if (loader != null) {
                    classLoader = loader.getClassLoader();
                }
                if (classLoader != null) {
                    ois = new CustomObjectInputStream(bis,
                            classLoader);
                } else {
                    ois = new ObjectInputStream(bis);
                }

                if (manager.getContainer().getLogger().isDebugEnabled()) {
                    manager.getContainer().getLogger().debug(sm.getString(getStoreName() + ".loading",
                            id, sessionTable));
                }

                _session = (StandardSession) manager.createEmptySession();
                _session.readObjectData(ois);
                _session.setManager(manager);
            } else if (manager.getContainer().getLogger().isDebugEnabled()) {
                manager.getContainer().getLogger().debug(getStoreName() + ": No persisted data object found");
            }
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
        } finally {
            try {
                if (preparedLoad != null) {
                    preparedLoad.close();
                }
                if (rst != null) {
                    rst.close();
                }
            } catch (SQLException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
            close(_conn);
        }

        return (_session);
    }

    /**
     * Remove the Session with the specified session identifier from
     * this Store, if present.  If no such Session is present, this method
     * takes no action.
     *
     * @param id Session identifier of the Session to be removed
     */
    @Override
    public void remove(String id) {
        Connection _conn = getConnection();
        if (_conn == null) {
            return;
        }

        try {
            remove(id, _conn);
        } finally {
            close(_conn);
        }

        if (manager.getContainer().getLogger().isDebugEnabled()) {
            manager.getContainer().getLogger().debug(sm.getString(getStoreName() + ".removing", id, sessionTable));
        }
    }

    /**
     * Remove the Session with the specified session identifier from
     * this Store, if present.  If no such Session is present, this method
     * takes no action.
     *
     * @param id    Session identifier of the Session to be removed
     * @param _conn open connection to be used
     */
    protected void remove(String id, Connection _conn) {
        PreparedStatement preparedRemove = null;

        try {
            preparedRemove = _conn.prepareStatement(preparedRemoveSql);
            preparedRemove.setString(1, id);
            preparedRemove.setString(2, getName());
            preparedRemove.execute();
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
        } finally {
            try {
                if (preparedRemove != null) {
                    preparedRemove.close();
                }
            } catch (SQLException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            }
        }
    }

    /**
     * Remove all of the Sessions in this Store.
     */
    @Override
    public void clear() {
        PreparedStatement preparedClear = null;

        Connection _conn = getConnection();
        if (_conn == null) {
            return;
        }

        try {
            preparedClear = _conn.prepareStatement(preparedClearSql);
            preparedClear.setString(1, getName());
            preparedClear.execute();
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
        } finally {
            try {
                if (preparedClear != null) {
                    preparedClear.close();
                }
            } catch (SQLException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            }
            close(_conn);
        }
    }

    /**
     * Save a session to the Store.
     *
     * @param session the session to be stored
     * @throws IOException if an input/output error occurs
     */
    @Override
    public void save(Session session) throws IOException {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;
        ByteArrayInputStream bis = null;
        InputStream in = null;
        PreparedStatement preparedSave = null;

        Connection _conn = getConnection();
        if (_conn == null) {
            return;
        }

        try {
            // If sessions already exist in DB, remove and insert again.
            // TODO:
            // * Check if ID exists in database and if so use UPDATE.
            remove(session.getIdInternal(), _conn);

            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(new BufferedOutputStream(bos));

            ((StandardSession) session).writeObjectData(oos);
            oos.close();
            oos = null;
            byte[] obs = bos.toByteArray();
            int size = obs.length;
            bis = new ByteArrayInputStream(obs, 0, size);
            in = new BufferedInputStream(bis, size);

            preparedSave = _conn.prepareStatement(preparedSaveSql);
            preparedSave.setString(1, session.getIdInternal());
            preparedSave.setString(2, getName());
            preparedSave.setBinaryStream(3, in, size);
            preparedSave.setString(4, session.isValid() ? "1" : "0");
            preparedSave.setInt(5, session.getMaxInactiveInterval());
            preparedSave.setLong(6, session.getLastAccessedTime());
            preparedSave.execute();
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
        } finally {
            try {
                if (preparedSave != null) {
                    preparedSave.close();
                }
            } catch (SQLException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".SQLException", e));
            }

            if (oos != null) {
                oos.close();
            }
            if (bis != null) {
                bis.close();
            }
            if (in != null) {
                in.close();
            }

            close(_conn);
        }

        if (manager.getContainer().getLogger().isDebugEnabled()) {
            manager.getContainer().getLogger().debug(sm.getString(getStoreName() + ".saving",
                    session.getIdInternal(), sessionTable));
        }
    }

    // --------------------------------------------------------- Protected Methods

    /**
     * Get a connection from a data source.
     * Returns <code>null</code> if the connection could not be established.
     *
     * @return <code>Connection</code> if the connection succeeded
     */
    protected Connection getConnection() {
        Connection conn = null;

        try {
            conn = getDataSource().getConnection();
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".checkConnectionSQLException", dataSourceName), e);
        }

        return conn;
    }

    /**
     * Close the specified database connection.
     *
     * @param conn The connection to be closed
     */
    protected void close(Connection conn) {
        // Commit if not auto committed
        try {
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".commitSQLException"), e);
        }

        try {
            conn.close();
        } catch (SQLException e) {
            manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".close", e.toString())); // Just log it here
        }
    }

    /**
     * Initialize dataSource if it hasn't been initialized yet.
     *
     * @return <code>DataSource</code>
     */
    protected DataSource getDataSource() {
        if (dataSource == null) {
            Context initCtx;
            try {
                initCtx = new InitialContext();
                Context envCtx = (Context) initCtx.lookup("java:comp/env");
                this.dataSource = (DataSource) envCtx.lookup(this.dataSourceName);
            } catch (NamingException e) {
                manager.getContainer().getLogger().error(sm.getString(getStoreName() + ".wrongDataSource", dataSourceName), e);
            }
        }

        return dataSource;
    }

    /**
     * Start this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
     *
     * @throws LifecycleException if this component detects a fatal error
     *                            that prevents this component from being used
     */
    @Override
    protected synchronized void startInternal() throws LifecycleException {
        // initialize DataSource
        this.dataSource = getDataSource();

        // Create the size PreparedStatement string
        preparedSizeSql = "SELECT COUNT(" + sessionIdCol + ") FROM " + sessionTable + " WHERE " + sessionAppCol + " = ?";

        // Create the keys PreparedStatement string
        preparedKeysSql = "SELECT " + sessionIdCol + " FROM " + sessionTable + " WHERE " + sessionAppCol + " = ?";

        // Create the save PreparedStatement string
        preparedSaveSql = "INSERT INTO " + sessionTable + " (" + sessionIdCol + ", " + sessionAppCol + ", "
                + sessionDataCol + ", " + sessionValidCol + ", " + sessionMaxInactiveCol + ", " + sessionLastAccessedCol
                + ") VALUES (?, ?, ?, ?, ?, ?)";

        // Create the clear PreparedStatement string
        preparedClearSql = "DELETE FROM " + sessionTable + " WHERE " + sessionAppCol + " = ?";

        // Create the remove PreparedStatement string
        preparedRemoveSql = "DELETE FROM " + sessionTable + " WHERE " + sessionIdCol + " = ?  AND " + sessionAppCol + " = ?";

        // Create the load PreparedStatement string
        preparedLoadSql = "SELECT " + sessionIdCol + ", " + sessionDataCol + " FROM " + sessionTable
                + " WHERE " + sessionIdCol + " = ? AND " + sessionAppCol + " = ?";

        super.startInternal();
    }

    /**
     * Stop this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#stopInternal()}.
     *
     * @throws LifecycleException if this component detects a fatal error
     *                            that prevents this component from being used
     */
    @Override
    protected synchronized void stopInternal() throws LifecycleException {
        super.stopInternal();

        this.dataSource = null;
    }
}
