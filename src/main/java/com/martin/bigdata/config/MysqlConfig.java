package com.martin.bigdata.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 * @author martin
 * @desc mysql 配置类
 */
@Configuration
@ConfigurationProperties(prefix = "mysql")
@MapperScan(basePackages = MysqlConfig.MAPPER_PACKAGE, sqlSessionFactoryRef = MysqlConfig.MYSQL_SQL_SESSION_FACTORY)
public class MysqlConfig {

    static final String MYSQL_SQL_SESSION_FACTORY = "mysqlSqlSessionFactory";
    static final String MAPPER_PACKAGE = "com.martin.bigdata.mapper.mysql";
    static final String MAPPER_DIR = "classpath:/mybatis-mapper/mysql/*";
    static final String ENTITY_PACKAGE = "com.martin.bigdata.pojo.entity";
    private String url;
    private String username;
    private String password;
    private String driverClass;

    @Bean(name = "mysqlDataSource")
    public DataSource mysqlDataSource() {

        // druid 数据源配置
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverClass);
        dataSource.setTestWhileIdle(true);
        dataSource.setValidationQuery("SELECT 1");
        return dataSource;
    }

    @Bean(name = "mysqlTransactionManager")
    public DataSourceTransactionManager mysqlTransactionManager(@Qualifier("mysqlDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = MYSQL_SQL_SESSION_FACTORY)
    public SqlSessionFactory mysqlSqlSessionFactory(@Qualifier("mysqlDataSource") DataSource mysqlDataSource)
            throws Exception {
        final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(mysqlDataSource);
        sessionFactory.setTypeAliasesPackage(ENTITY_PACKAGE);
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources(MysqlConfig.MAPPER_DIR));
        return sessionFactory.getObject();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }
}
