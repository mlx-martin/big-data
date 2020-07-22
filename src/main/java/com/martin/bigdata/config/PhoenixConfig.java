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
import java.util.Properties;

/**
 * @author martin
 * @desc phoenix 连接 hbase 配置类
 */
@Configuration
@ConfigurationProperties(prefix = "hbase.phoenix")
@MapperScan(basePackages = PhoenixConfig.PACKAGE, sqlSessionFactoryRef = PhoenixConfig.PHOENIX_SQL_SESSION_FACTORY)
public class PhoenixConfig {

    static final String PHOENIX_SQL_SESSION_FACTORY = "phoenixSqlSessionFactory";
    static final String PACKAGE = "com.martin.bigdata.mapper";
    static String MAPPER_LOCATION = "classpath:/mybatis-mapper/*";
    private String url;
    private String driverClass;
    private String schemaIsNamespaceMappingEnabled;


    @Bean(name = "phoenixDataSource")
    public DataSource phoenixDataSource() {

        // druid 数据源配置
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName(driverClass);
        dataSource.setTestWhileIdle(true);
        dataSource.setValidationQuery("SELECT 1");

        // phoenix 配置
        Properties phoenixConfig = new Properties();
        phoenixConfig.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        dataSource.setConnectProperties(phoenixConfig);
        return dataSource;
    }

    @Bean(name = "hbasePhoenixTransactionManager")
    public DataSourceTransactionManager hbasePhoenixTransactionManager(@Qualifier("phoenixDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = PHOENIX_SQL_SESSION_FACTORY)
    public SqlSessionFactory phoenixSqlSessionFactory(@Qualifier("phoenixDataSource") DataSource phoenixDataSource)
            throws Exception {
        final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(phoenixDataSource);
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources(PhoenixConfig.MAPPER_LOCATION));
        return sessionFactory.getObject();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getSchemaIsNamespaceMappingEnabled() {
        return schemaIsNamespaceMappingEnabled;
    }

    public void setSchemaIsNamespaceMappingEnabled(String schemaIsNamespaceMappingEnabled) {
        this.schemaIsNamespaceMappingEnabled = schemaIsNamespaceMappingEnabled;
    }
}
