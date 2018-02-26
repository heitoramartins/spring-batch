package br.com.spring.batch.items;

import br.com.spring.batch.entities.User;
import org.springframework.batch.item.ItemProcessor;


import java.sql.Timestamp;
import java.util.Date;

public class CustomItemProcessor implements ItemProcessor<User, User>{

    @Override
    public User process(User user) throws Exception {

        Timestamp date = new Timestamp(new Date().getTime());

        user.setDate(date);

        return user;
    }
}
