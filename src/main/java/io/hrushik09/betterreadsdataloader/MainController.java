package io.hrushik09.betterreadsdataloader;

import io.hrushik09.betterreadsdataloader.author.Author;
import io.hrushik09.betterreadsdataloader.author.AuthorRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;

@Controller
public class MainController {
    @Autowired
    AuthorRepository authorRepository;

    @PostConstruct
    public void start() {
        Author author = new Author();
        author.setId("id2");
        author.setName("name2");
        author.setPersonalName("personalName2");

        authorRepository.save(author);
    }
}
