package io.hrushik09.betterreadsdataloader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.hrushik09.betterreadsdataloader.author.Author;
import io.hrushik09.betterreadsdataloader.author.AuthorRepository;
import io.hrushik09.betterreadsdataloader.book.Book;
import io.hrushik09.betterreadsdataloader.book.BookRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller
public class MainController {
    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${aws-s3-accesskey}")
    private String accessKey;

    @Value("${aws-s3-secretkey}")
    private String secretKey;

    @Value("${aws-s3-bucket-name}")
    private String bucketName;

    @Value("${aws-s3-key-name}")
    private String keyName;

    @PostConstruct
    public void start() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object object = s3client.getObject(new GetObjectRequest(bucketName, keyName));
        InputStream objectData = object.getObjectContent();

        initAuthors(objectData);
//        initWorks(objectData);
    }

    private void initAuthors(InputStream objectData) {
        System.out.println("working on " + bucketName + " - " + keyName);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(objectData))) {
            String line;

            while ((line = reader.readLine()) != null) {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Construct the Author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    System.out.println("saved author " + author.getName());
                    // Persist using Repository
                    authorRepository.save(author);
                } catch (JSONException ignored) {
                }
            }

            System.out.println(bucketName + " - " + keyName + " was uploaded");
            objectData.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks(InputStream objectData) {
        System.out.println("working on " + bucketName + " - " + keyName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(objectData))) {
            String line;

            while ((line = reader.readLine()) != null) {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Construct the Book object
                    Book book = new Book();

                    book.setId(jsonObject.getString("key").replace("/works/", ""));

                    book.setName(jsonObject.optString("title"));

                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if (descriptionObj != null) {
                        book.setDescription(descriptionObj.optString("value"));
                    }

                    JSONObject publishedObj = jsonObject.optJSONObject("created");
                    if (publishedObj != null) {
                        String dateStr = publishedObj.optString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, dateTimeFormatter));
                    }

                    JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
                    if (coversJSONArr != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversJSONArr.length(); i++) {
                            coverIds.add(coversJSONArr.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
                    if (authorsJSONArr != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < authorsJSONArr.length(); i++) {
                            String authorId = authorsJSONArr.getJSONObject(i)
                                    .getJSONObject("author")
                                    .getString("key")
                                    .replace("/authors/", "");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);

                        List<String> authorNames = authorIds.stream()
                                .map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (optionalAuthor.isEmpty()) {
                                        return "Unknown Author";
                                    }

                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }

                    System.out.println("saved book " + book.getName());
                    // Persist using Repository
                    bookRepository.save(book);
                } catch (JSONException ignored) {
                }
            }

            System.out.println(bucketName + " - " + keyName + " was uploaded");
            objectData.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
