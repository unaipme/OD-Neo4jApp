package eus.unai.neo4j;

import org.neo4j.driver.v1.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jApp implements AutoCloseable {

    // Article.csv
    // id;author;author-aux;author-orcid;booktitle;cdate;cdrom;cite;cite-label;crossref;editor;editor-orcid;ee;ee-type;i;journal;key;mdate;month;note;note-type;number;pages;publisher;publtype;sub;sup;title;title-bibtex;tt;url;volume;year
    // 0  1      2          3            4         5     6     7    8          9        10     11           12 13      14 15     16  17    18    19   20        21     22    23        24       25  26  27    28           29 30  31     32

    // Proceedings.csv
    // id;address;author;booktitle;cite;cite-label;crossref;editor;editor-orcid;ee;ee-type;i;isbn;isbn-type;journal;key;mdate;note;note-type;number;pages;publisher;publisher-href;publtype;series;series-href;sub;sup;title;url;volume;year
    // 0  1       2      3         4    5          6        7      8            9  10      11 12  13        14      15  16    17   18        19     20    21        22             23       24     25          26  27  28    29  30     31

    // Inproceedings.csv
    // id;author;author-orcid;booktitle;cdrom;cite;cite-label;crossref;editor;editor-orcid;ee;ee-type;i;key;mdate;month;note;note-type;number;pages;publtype;sub;sup;title;title-bibtex;tt;url;year
    // 0  1      2            3         4     5    6          7        8      9            10 11      12 13 14    15    16   17        18     19    20       21  22  23    24           25 26  27

    private Driver driver;



    public Neo4jApp() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687");
    }

    public static void main(String [] args) throws Exception {
        if (args.length == 0) throw new Exception("At least one argument required: { populate | query }");
        try (Neo4jApp app = new Neo4jApp()) {
            switch (args[0].toLowerCase()) {
                case "populate":
                    app.populate(args);
                    break;
                case "query":
                    app.query(args);
                    break;
            }
        }
    }

    private void populate(String [] args) throws Exception {
        if (args.length == 1) {
            System.out.println("Available options:\n");
            System.out.println(String.format("\u001B[36m%s\t\t\t\t\t\u001B[34m%s\u001B[0m", "all", "Inserts all data related to articles and inproceedings"));
            System.out.println(String.format("\u001B[36m%s\t\t\u001B[34m%s\u001B[0m", "all-articles", "Inserts all data related to articles"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "all-inproc", "Inserts all data related to inproceedings"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "articles", "Inserts articles and creates relationships with the journal volume"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\t\u001B[34m%s\u001B[0m", "authors", "Inserts article authors"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "citations", "Generates citation relationships from the file"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "conferences", "Inserts all data related to conferences"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\t\u001B[34m%s\u001B[0m", "inproc", "Inserts all inproceedings articles and creates relationships with the proceedings"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "journals", "Inserts all data related to journals"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "keywords", "Creates keywords and randomly generates relationships"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "proceedings", "Inserts all proceedings and creates the relationship with its conference"));
            System.out.println(String.format("\u001B[36m%s\t\t\u001B[34m%s\u001B[0m", "rnd-citations", "Generates randomly citation relationships"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\t\u001B[34m%s\u001B[0m", "volumes", "Inserts all volumes and their journals"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "written-by", "Creates the relationships between authors and journal articles"));
            System.out.println(String.format("\u001B[36m%s\t\u001B[34m%s\u001B[0m", "written-by-inproc", "Creates the relationships between authors and inproceedings articles"));
        } else {
            switch (args[1].toLowerCase()) {
                case "all":
                    this.insertArticles();
                    this.insertInProceedings();
                    break;
                case "all-articles":
                    this.insertArticles();
                    break;
                case "all-inproc":
                    this.insertInProceedings();
                    break;
                case "articles":
                    System.out.println("Journals and volumes must also be inserted in this mode");
                    this.insertJournals();
                    this.insertVolumes();
                    this.insertJournalArticles();
                    break;
                case "authors":
                    this.insertAuthors();
                    break;
                case "citations":
                    this.createArticleCitations();
                    this.createInproceedingsCitations();
                    break;
                case "conferences":
                    this.insertConferences();
                    break;
                case "inproc":
                    System.out.println("Conferences and proceedings must also be inserted in this mode");
                    this.insertConferences();
                    this.insertProceedings();
                    this.insertInProceedings();
                    break;
                case "journals":
                    this.insertJournals();
                    break;
                case "keywords":
                    this.generateRandomKeywords();
                    break;
                case "model1":
                    this.insertConferences();
                    this.insertJournals();
                    this.insertProceedings();
                    this.insertVolumes();
                    this.insertInProceedingsAuthors();
                    this.insertAuthors();
                    this.insertInProceedingsArticles();
                    this.insertArticles();
                    this.generateRandomKeywords();
                    this.generateRandomReviewEdges();
                    this.generateRandomCitations();
                    break;
                case "model2":
                    this.insertConferences();
                    this.insertJournals();
                    this.insertProceedings();
                    this.insertVolumes();
                    this.insertInProceedingsAuthors();
                    this.insertAuthors();
                    this.insertInProceedingsArticles();
                    this.insertArticles();
                    this.generateRandomKeywords();
                    this.generateRandomReviews();
                    this.generateRandomCitations();
                    this.generateRandomOrganizations();
                    break;
                case "proceedings":
                    System.out.println("Conferences must be also inserted in this mode");
                    this.insertConferences();
                    this.insertProceedings();
                    break;
                case "reviews":
                    this.generateRandomReviews();
                    break;
                case "rnd-citations":
                    this.generateRandomCitations();
                    break;
                case "volumes":
                    System.out.println("Journals must be also inserted in this mode");
                    this.insertJournals();
                    this.insertVolumes();
                    break;
                case "written-by":
                    this.mergeArticleAndAuthor();
                    break;
                case "written-by-inproc":
                    this.mergeProceedingsAuthorAndArticles();
                    break;
            }
        }
    }

    private void query(String [] args) throws Exception {
        if (args.length == 1) {
            System.out.println(String.format("\u001B[36m%s\t\t\u001B[34m%s\u001B[0m", "community", "Find the community of specified conference or show all communities"));
            System.out.println(String.format("\u001B[36m%s\t\t\t\u001B[34m%s\u001B[0m", "hindex", "Calculate hindex for specified author or show to 100 highest hindex-es"));
            System.out.println(String.format("\u001B[36m%s\t\u001B[34m%s\u001B[0m", "impact-factor", "Calculate impact factor for specified journal or show 100 highest impact factors"));
            System.out.println(String.format("\u001B[36m%s\t\t\u001B[34m%s\u001B[0m", "most-cited", "Find the most cited articles of a specified conference or of all of them"));
        } else {
            switch (args[1].toLowerCase()) {
                case "community":
                    if (args.length == 3) {
                        this.community(args[2]);
                    } else {
                        this.community();
                    }
                    break;
                case "hindex":
                    if (args.length == 3) {
                        this.hIndex(args[2]);
                    } else {
                        this.hIndex();
                    }
                    break;
                case "impact-factor":
                    if (args.length == 3) {
                        this.impactFactor(args[2]);
                    } else {
                        this.impactFactor();
                    }
                    break;
                case "most-cited":
                    if (args.length == 3) {
                        this.mostCited(args[2]);
                    } else {
                        this.mostCited();
                    }
                    break;
            }
        }
    }

    private void insertArticles() throws IOException {
        this.insertAuthors();
        this.insertJournals();
        this.insertVolumes();
        this.insertJournalArticles();
        this.mergeArticleAndAuthor();
    }

    private void insertInProceedings() throws IOException {
        this.insertConferences();
        this.insertProceedings();

        this.insertInProceedingsAuthors();
        this.insertInProceedingsArticles();
        this.mergeProceedingsAuthorAndArticles();
    }

    private void insertAuthors() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_article.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(50000).forEach(l -> {
                String [] authors = l.split(";")[1].split("\\|");
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("UNWIND $authors AS name " +
                                "MERGE (:Author {name: name})",
                                parameters("authors", authors)
                ));
                System.out.println("Authors " + String.join(", ", authors) + " created");
            });
        }
        System.out.println("Done");
    }

    private void insertJournals() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_article.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            int [] reviewersOptions = new int [] {3, 3, 3, 3, 3, 3, 3, 3, 4, 5};
            Random random = new Random();
            br.lines().skip(1).limit(50000).forEach(l -> {
                int numberReviewers = reviewersOptions[random.nextInt(reviewersOptions.length - 1)];
                String journal = l.split(";")[15];
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("MERGE (j:Journal {name: $journal})" +
                                "SET j.numberReviewers = $numberReviewers",
                                parameters("journal", journal, "numberReviewers", numberReviewers)
                ));
                System.out.println("Journal '" + journal + "' created");
            });
        }
        System.out.println("Done");
    }

    private void insertVolumes() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_article.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(50000).forEach(l -> {
                try {
                    String[] columns = l.split(";");
                    String journal = columns[15];
                    Integer volume = Integer.parseInt(columns[31]);
                    Integer year = Integer.parseInt(columns[32]);
                    StatementResult result = session.writeTransaction(tx ->
                            tx.run("MATCH (j:Journal {name: $journal}) " +
                                    "MERGE (v:Volume {number: $volume})<-[:EDITION]-(j) " +
                                    "ON CREATE SET v.year = $year",
                                    parameters("volume", volume, "journal", journal, "year", year)
                            ));
                    System.out.println("Volume '" + volume + "' created");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Done");
    }

    private void insertJournalArticles() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_article.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(50000).forEach(l -> {
                String [] columns = l.split(";");
                try {
                    String journal = columns[15];
                    Integer volume = Integer.parseInt(columns[31]);
                    String article = columns[27];
                    String key = columns[16];
                    StatementResult result = session.writeTransaction(tx ->
                            tx.run("MATCH (v:Volume {number: $volume})<-[:EDITION]-(j:Journal {name: $journal}) " +
                                    "MERGE (a:Article {title: $article})<-[:PUBLISHED]-(v) " +
                                    "ON CREATE SET a.key = $key",
                                    parameters("volume", volume, "journal", journal,
                                            "article", article, "key", key))
                    );
                    System.out.println("Article '" + article + "' created");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Done");
    }

    private void mergeArticleAndAuthor() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_article.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(50000).forEach(l -> {
                String [] columns = l.split(";");
                try {
                    String [] authors = columns[1].split("\\|");
                    String article = columns[27];
                    StatementResult result = session.writeTransaction(tx ->
                        tx.run("UNWIND $authors AS author " +
                                "MATCH (au:Author {name: author}), (art:Article {title: $article}) " +
                                "MERGE (art)-[:WRITTEN_BY]->(au)",
                                parameters("authors", authors, "article", article)
                    ));
                    System.out.println("Relationship between article '" + article + "' and its authors created");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Done");
    }

    private void createArticleCitations() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_article.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(50000).forEach(l -> {
                String [] columns = l.split(";");
                try {
                    String [] citations = columns[5].split("\\|");
                    String article = columns[27];
                    StatementResult result = session.writeTransaction(tx ->
                            tx.run("UNWIND $citations AS cite " +
                                    "MATCH (citing:Article {title: $article}), (cited:Article {key: cite}) " +
                                    "MERGE (cited)-[:CITED_BY]->(citing)",
                                    parameters("citations", citations, "article", article)
                            ));
                    System.out.println("Citations of article '" + article + "' created");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Done");
    }

    private void insertConferences() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_proceedings.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).forEach(l -> {
                String [] values = l.split(";");
                String name = values[3];
                if (name == null || "".equals(name)) return;
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("MERGE (:Conference {name: $name})",
                                parameters("name", name))
                );
                System.out.println("Conference '" + name + "' introduced.");
            });
        }
        System.out.println("Done");
    }

    private void insertProceedings() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_proceedings.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).forEach(l -> {
                String [] values = l.split(";");
                String booktitle = values[3];
                String title = values[28];
                String key = values[15];
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("MATCH (c:Conference {name: $booktitle}) " +
                                "MERGE (c)-[:EDITION]->(:Proceedings {title: $title, key: $key})",
                                parameters("booktitle", booktitle, "title", title, "key", key))
                );
                System.out.println("Conference proceeding '" + title + "' introduced.");
            });
        }
        System.out.println("Done");
    }

    private void insertInProceedingsAuthors() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_inproceedings.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(20000).forEach(l -> {
                String [] values = l.split(";");
                List<String> authorList = Arrays.asList(values[1].split("\\|"));
                authorList.forEach(name -> {
                    StatementResult result = session.writeTransaction(tx ->
                            tx.run("MERGE (:Author {name: $name})",
                                    parameters("name", name))
                    );
                    System.out.println("Author '" + name + "' introduced.");
                });
            });
        }
        System.out.println("Done");
    }

    private void insertInProceedingsArticles() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_inproceedings.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(20000).forEach(l -> {
                String [] values = l.split(";");
                String crossref = values[7];
                String title = values[23];
                String key = values[13];
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("MATCH (p:Proceedings {key: $crossref}) " +
                                        "MERGE (a:Article {title: $title})<-[:PUBLISHED]-(p) " +
                                        "ON CREATE SET a.key = $key",
                                parameters("crossref", crossref, "title", title, "key", key))
                );
                System.out.println("Article '" + title + "' introduced.");
            });
        }
        System.out.println("Done");
    }

    private void mergeProceedingsAuthorAndArticles() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_inproceedings.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(20000).forEach(l -> {
                String [] values = l.split(";");
                String title = values[23];
                String [] authors = values[1].split("\\|");
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("UNWIND $authors AS authorName " +
                                "MATCH (author:Author {name: authorName}), (art:Article {title: $title}) " +
                                "MERGE (art)-[:WRITTEN_BY]->(author)",
                                parameters("authors", authors, "title", title))
                );
                System.out.println("Relationship between article '" + title + "' and its authors introduced.");
            });
        }
        System.out.println("Done");
    }

    private void createInproceedingsCitations() throws IOException {
        try (Session session = driver.session();
             InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get("dblp_inproceedings.csv")));
             BufferedReader br = new BufferedReader(isr)) {
            br.lines().skip(1).limit(20000).forEach(l -> {
                String [] columns = l.split(";");
                try {
                    String [] citations = columns[7].split("\\|");
                    String title = columns[23];
                    StatementResult result = session.writeTransaction(tx ->
                            tx.run("UNWIND $citations AS cite " +
                                            "MATCH (citing:Article {title: $title}), (cited:Article {key: cite}) " +
                                            "MERGE (cited)-[:CITED_BY]->(citing)",
                                    parameters("citations", citations, "title", title)
                            ));
                    System.out.println("Citations of inproceedings '" + title + "' created");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Done");
    }

    private void generateRandomCitations() throws IOException {
        try (Session session = driver.session()) {
            Random random = new Random();
            for (int i = 0; i < 10000; i++) {
                int amount = random.nextInt(6) + 1;
                session.writeTransaction(tx ->
                        tx.run("MATCH (a:Article) WITH a AS citing SKIP toInt(round(rand() * 68000)) LIMIT 1 " +
                                "MATCH (a:Article) WITH a AS cited, citing SKIP toInt(round(rand() * 68000)) " +
                                "WHERE citing <> cited LIMIT $amount " +
                                "MERGE (cited)-[:CITED_BY]->(citing)",
                                parameters("amount", amount))
                );
                System.out.println(amount + " new citations generated");
            }
        }
        System.out.println("Done");
    }

    private void generateRandomKeywords() {
        try (Session session = driver.session()) {
            String [] keywords = new String [] {"data management", "indexing", "data modeling", "big data",
                    "data processing", "data storage", "data querying"};
            session.writeTransaction(tx ->
                    tx.run("UNWIND $keywords AS keyword " +
                            "MERGE (:Keyword {key: keyword})",
                            parameters("keywords", keywords))
            );
            for (int i = 0; i < 30000; i++) {
                session.writeTransaction(tx ->
                        tx.run("MATCH (a:Article) WITH a AS article SKIP toInt(round(rand() * 69000)) LIMIT 1 " +
                                "MATCH (k:Keyword) WITH article, k SKIP toInt(round(rand() * 7)) LIMIT 1 " +
                                "MERGE (k)-[:RELATES]->(article)")
                );
                System.out.println("Relationship between keyword and article created. " + (30000 - i) + " to go.");
            }
        }
        System.out.println("Done");
    }

    private void generateRandomReviewEdges() {
        try (Session session = driver.session()) {
            for (int i = 0; i < 10000; i++) {
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("MATCH (j:Journal)-[:EDITION]->(:Volume)-[:PUBLISHED]->(a:Article) " +
                                "WITH j, a SKIP toInt(rand() * 49000) LIMIT 1 " +
                                "MATCH (auth:Author) WHERE NOT (a)-[:WRITTEN_BY]->(auth) " +
                                "WITH j, a, auth SKIP toInt(rand() * 115000) LIMIT 5 " +
                                "FOREACH (author IN authors | MERGE (author)-[:REVIEWS]->(a))")
                );
                System.out.println("New review created. " + (30000 - i) +  " to go.");
            }
        }
        System.out.println("Done");
    }

    private void generateRandomReviews() {
        try (Session session = driver.session()) {
            Random random = new Random();
            String [] grades = new String [] {"rejected", "accepted", "accepted", "accepted", "accepted"};
            for (int i = 0; i < 10000; i++) {
                StatementResult result = session.writeTransaction(tx ->
                        tx.run("MATCH (j:Journal)-[:EDITION]->(:Volume)-[:PUBLISHED]->(a:Article) " +
                                "WHERE NOT (a)<-[:ABOUT]-(:Review) WITH j, a SKIP toInt(rand() * 49000) LIMIT 1 " +
                                "MATCH (auth:Author) WHERE NOT (a)-[:WRITTEN_BY]->(auth) " +
                                "WITH j, a, auth SKIP toInt(rand() * 115000) LIMIT 5 " +
                                "WITH a, COLLECT(auth)[..j.numberReviewers] AS authors " +
                                "FOREACH (author IN authors | MERGE (author)-[:WRITES]->(:Review {grade: $grade})-[:ABOUT]->(a))",
                                parameters("grade", grades[random.nextInt(grades.length)]))
                );
                System.out.println("New review created. " + (30000 - i) +  " to go.");
            }
        }
        System.out.println("Done");
    }

    private void transformReviewEdgesIntoNodes() {
        try (Session session = driver.session()) {
            session.writeTransaction(tx ->
                    tx.run("MATCH (author:Author)-[r:REVIEWS]->(article:Article) " +
                            "WITH author, article, " +
                            "['rejected', 'accepted', 'accepted', 'accepted', 'accepted'][toInteger(rand() * 5)] AS grade" +
                            "MERGE (author)-[:WRITES]->(:Review {grade: grade})-[:ABOUT]->(a) " +
                            "DETACH DELETE r")
            );
        }
    }

    private void generateRandomOrganizations() {

    }

    private void hIndex() {
        try (Session session = driver.session()) {
            StatementResult result = session.writeTransaction(tx ->
                tx.run("MATCH (author:Author)<-[:WRITTEN_BY]-(a:Article)-[c:CITED_BY]->(:Article) " +
                        "WITH author, a, COUNT(c) AS citeCount ORDER BY citeCount DESC " +
                        "WITH author, COLLECT(citeCount) AS citations " +
                        "RETURN author.name, SIZE(FILTER(i IN RANGE(0, SIZE(citations) - 1) WHERE citations[i] > i)) AS hIndex " +
                        "ORDER BY hIndex DESC LIMIT 100;")
            );
            System.out.println("Showing at most 100 results");
            result.stream().forEach(r -> System.out.println(String.format("%s: %d", r.get(0).asString(), r.get(1).asInt())));
        }
    }

    private void hIndex(String name) {
        try (Session session = driver.session()) {
            Record record = session.writeTransaction(tx ->
                    tx.run("MATCH (author:Author {name: $name})<-[:WRITTEN_BY]-(a:Article)-[c:CITED_BY]->(:Article) " +
                                    "WITH author, a, COUNT(c) AS citeCount ORDER BY citeCount DESC " +
                                    "WITH author, COLLECT(citeCount) AS citations " +
                                    "RETURN author.name, SIZE(FILTER(i IN RANGE(0, SIZE(citations) - 1) WHERE citations[i] > i)) AS hIndex",
                            parameters("name", name))

            ).single();
            System.out.println(String.format("%s: %d", record.get(0).asString(), record.get(1).asInt()));
        }
    }

    private void community() {
        try (Session session = driver.session()) {
            session.writeTransaction(tx ->
                tx.run("MATCH (c:Conference)-[:EDITION]->(p:Proceedings)-[:PUBLISHED]->(a:Article)-[:WRITTEN_BY]->(author:Author) " +
                        "WITH author, c, COUNT(DISTINCT p) AS amount " +
                        "ORDER BY amount DESC WHERE amount >= 4 RETURN c.name, COLLECT(author.name)")
            ).stream().forEach(r ->
                System.out.println(String.format("Conference %s: %s", r.get(0).asString(),
                        r.get(1).asList().stream().map(Object::toString).collect(Collectors.joining(", "))))
            );
        }
    }

    private void community(String name) {
        try (Session session = driver.session()) {
            Record record = session.writeTransaction(tx ->
                    tx.run("MATCH (c:Conference {name: $name})-[:EDITION]->(p:Proceedings)-[:PUBLISHED]->(a:Article)-[:WRITTEN_BY]->(author:Author) " +
                            "WITH author, c, COUNT(DISTINCT p) AS amount " +
                            "ORDER BY amount DESC WHERE amount >= 4 RETURN c.name, COLLECT(author.name)",
                            parameters("name", name))
            ).single();
            System.out.println(String.format("Conference %s: %s", record.get(0).asString(),
                    record.get(1).asList().stream().map(Object::toString).collect(Collectors.joining(", "))));
        }
    }

    private void impactFactor() {
        try (Session session = driver.session()) {
            StatementResult result = session.writeTransaction(tx ->
                    tx.run("MATCH (j:Journal)-[:EDITION]->(v:Volume)-[:PUBLISHED]->(a:Article) " +
                            "OPTIONAL MATCH (a)-[c:CITED_BY]->(:Article) " +
                            "WHERE v.year IN [date().year - 1, date().year - 2] " +
                            "WITH j, a, COUNT(c) AS citations " +
                            "RETURN j.name, toFloat(SUM(citations)) / toFloat(COUNT(a)) AS impactFactor " +
                            "ORDER BY impactFactor DESC LIMIT 100")
            );
            System.out.println("Showing at most 100 results");
            result.stream().forEach(r -> System.out.println(String.format("%s: %d", r.get(0).asString(), r.get(1).asInt())));
        }
    }

    private void impactFactor(String name) {
        try (Session session = driver.session()) {
            Record record = session.writeTransaction(tx ->
                    tx.run("MATCH (j:Journal {name: $name})-[:EDITION]->(v:Volume)-[:PUBLISHED]->(a:Article) " +
                            "OPTIONAL MATCH (a)-[c:CITED_BY]->(:Article) " +
                            "WHERE v.year IN [date().year - 1, date().year - 2] " +
                            "WITH j, a, COUNT(c) AS citations " +
                            "RETURN j.name, toFloat(SUM(citations)) / toFloat(COUNT(a)) AS impactFactor",
                            parameters("name", name))
            ).single();
            System.out.println(String.format("%s: %d", record.get(0).asString(), record.get(1).asInt()));
        }
    }

    private void mostCited() {
        try (Session session = driver.session()) {
            session.writeTransaction(tx ->
                    tx.run("MATCH (c:Conference)-[:EDITION]->(:Proceedings)" +
                            "-[:PUBLISHED]->(a:Article)-[cite:CITED_BY]->(:Article) " +
                            "WITH c, a, COUNT(cite) AS cites ORDER BY cites DESC " +
                            "RETURN c.name, COLLECT(a.title)[..3] AS mostCited")
            ).stream().forEach(r -> System.out.println(String.format("%s: %s", r.get(0).asString(),
                    r.get(1).asList().stream().map(Object::toString).collect(Collectors.joining(", ")))));
        }
    }

    private void mostCited(String name) {
        try (Session session = driver.session()) {
            Record record = session.writeTransaction(tx ->
                    tx.run("MATCH (c:Conference {name: $name})-[:EDITION]->(:Proceedings)" +
                            "-[:PUBLISHED]->(a:Article)-[cite:CITED_BY]->(:Article) " +
                            "WITH c, a, COUNT(cite) AS cites ORDER BY cites DESC " +
                            "RETURN c.name, COLLECT(a.title)[..3] AS mostCited",
                            parameters("name", name))
            ).single();
            System.out.println(String.format("%s: %s", record.get(0).asString(),
                    record.get(1).asList().stream().map(Object::toString).collect(Collectors.joining(", "))));
        }
    }

    @Override
    public void close() {
        driver.close();
    }

}