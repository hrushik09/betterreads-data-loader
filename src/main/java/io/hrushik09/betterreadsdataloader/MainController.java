package io.hrushik09.betterreadsdataloader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.hrushik09.betterreadsdataloader.author.Author;
import io.hrushik09.betterreadsdataloader.author.AuthorRepository;
import io.hrushik09.betterreadsdataloader.book.Book;
import io.hrushik09.betterreadsdataloader.book.BookRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class MainController {
    AuthorRepository authorRepository;
    BookRepository bookRepository;

    public MainController(AuthorRepository authorRepository, BookRepository bookRepository) {
        this.authorRepository = authorRepository;
        this.bookRepository = bookRepository;
    }

    @Value("${aws-s3-accesskey}")
    private String accessKey;
    @Value("${aws-s3-secretkey}")
    private String secretKey;
    @Value("${aws-s3-authors-bucket}")
    private String authorsBucket;
    @Value("${aws-s3-works-bucket}")
    private String worksBucket;
    @Value("${aws-s3-test-bucket}")
    private String testBucket;
    @Value("${aws-s3-authors-key}")
    private String authorsKey;
    @Value("${aws-s3-works-key}")
    private String worksKey;

    @PostConstruct
    public void start() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        uploadAuthors(s3client);
        uploadWorks(s3client);
    }

    private void uploadAuthors(AmazonS3 s3client) {
        ObjectListing authorsObjectListing = s3client.listObjects(authorsBucket);
        List<String> authorsKeyList = new ArrayList<>();
        for (S3ObjectSummary os : authorsObjectListing.getObjectSummaries()) {
            authorsKeyList.add(os.getKey());
        }

        for (String authorsKey : authorsKeyList) {
            S3Object object = s3client.getObject(new GetObjectRequest(authorsBucket, authorsKey));
            InputStream objectData = object.getObjectContent();
            initAuthors(objectData, authorsBucket, authorsKey);
        }
    }

    private void uploadWorks(AmazonS3 s3client) {
        ObjectListing worksObjectListing = s3client.listObjects(worksBucket);
        List<String> worksKeyList = new ArrayList<>();
        for (S3ObjectSummary os : worksObjectListing.getObjectSummaries()) {
            worksKeyList.add(os.getKey());
        }

        for (String worksKey : worksKeyList) {
            S3Object object = s3client.getObject(new GetObjectRequest(worksBucket, worksKey));
            InputStream objectData = object.getObjectContent();
            initWorks(objectData, worksBucket, worksKey);
        }
    }

    private void initAuthors(InputStream objectData, String bucketName, String keyName) {
        System.out.println("Uploading Authors: bucket - " + bucketName + ", key - " + keyName);
        long count = 1L;
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

                    // Persist using Repository
                    authorRepository.save(author);
                    System.out.println("Added Author " + count + ": bucket - " + bucketName + ", key - " + keyName + ", Author - " + author.getName());
                    count++;
                } catch (JSONException ignored) {
                }
            }

            objectData.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks(InputStream objectData, String bucketName, String keyName) {
        System.out.println("Uploading Works: bucket - " + bucketName + ", key - " + keyName);
        long count = 1L;
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
                        for (int i = 0; i < 15 && i < coversJSONArr.length(); i++) {
                            coverIds.add(coversJSONArr.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
                    if (authorsJSONArr != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < 15 && i < authorsJSONArr.length(); i++) {
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

                    // Persist using Repository
                    bookRepository.save(book);
                    System.out.println("Added Work " + count + ": bucket - " + bucketName + ", key - " + keyName + ", Work - " + book.getName());
                    count++;
                } catch (JSONException | DateTimeParseException ignored) {
                }
            }

            objectData.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void test(AmazonS3 s3client) {
        S3Object authorsObject = s3client.getObject(new GetObjectRequest(testBucket, "test-authors.txt"));
        InputStream authorsObjectData = authorsObject.getObjectContent();
        initAuthors(authorsObjectData, testBucket, "test-authors.txt");

        S3Object worksObject = s3client.getObject(new GetObjectRequest(testBucket, "test-works.txt"));
        InputStream worksObjectData = worksObject.getObjectContent();
        initWorks(worksObjectData, testBucket, "test-works.txt");
    }
}
