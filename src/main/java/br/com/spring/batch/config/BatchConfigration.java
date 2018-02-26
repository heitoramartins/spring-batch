package br.com.spring.batch.config;

import br.com.spring.batch.entities.User;
import br.com.spring.batch.items.CustomItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@EnableTransactionManagement
public class BatchConfigration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ResourceLoader resourceLoader;


    public DataSource dataSource() {
        final DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl("jdbc:mysql://localhost/spring-batch");
        ds.setUsername("root");
        ds.setPassword("root");
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        return ds;
    }


    @Bean
    public StaxEventItemReader<User> reader() {

        StaxEventItemReader<User> reader = new StaxEventItemReader<User>();

        Resource resource = resourceLoader.getResource("/home/heitor/projetos/spring-batch/src/main/resources/user.xml");
        reader.setResource(resource);
        reader.setFragmentRootElementName("user");

        Map<String, String> aliases = new HashMap<>();
        aliases.put("user", "br.com.spring.batch.entities.User");

        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(aliases);

        reader.setUnmarshaller(marshaller);

        return reader;
    }

    @Bean
    public CustomItemProcessor processor() {
        return new CustomItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<User> writer() {

        JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<>();

        writer.setDataSource(dataSource);

        writer.setSql("insert into user(name, create_date) values (?,?)");

        writer.setItemPreparedStatementSetter(new UserItemPreparedStm());

        return writer;
    }


    private class UserItemPreparedStm implements ItemPreparedStatementSetter<User> {

        @Override
        public void setValues(User user, PreparedStatement ps) throws SQLException {
            ps.setString(1, user.getName());
            ps.setTimestamp(1, user.getDate());
        }
    }


    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<User, User>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }


    @Bean
    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }


}
