package com.example.demo.ImportCSVs;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
@PropertySource("classpath:application.properties")
public class CsvToHiveImporter {

    @Value("${csv.file.path}")
    private String csvFilePath;

    @Value("${hive.url}")
    private String url;

//    @Value("${hive.user}")
//    private String user;
//
//    @Value("${hive.password}")
//    private String password;

    Path tempCsvFile = Paths.get("data/student_grades_noheader.csv").toAbsolutePath();

    public DataSource getHiveDataSource() {

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
//        dataSource.setUsername(user);
//        dataSource.setPassword(password);

        return dataSource;
    }

    public JdbcTemplate getJDBCTemplate() throws IOException {
        return new JdbcTemplate(getHiveDataSource());
    }

    public void importFile() throws IOException {
        JdbcTemplate jdbcTemplate = getJDBCTemplate();
        try (
                CSVReader reader = new CSVReader(new FileReader(csvFilePath));
                BufferedWriter writer = Files.newBufferedWriter(tempCsvFile);
        ) {
            // Drop the table if it exists
            String dropTableSQL = "DROP TABLE IF EXISTS student_grades";
            jdbcTemplate.execute(dropTableSQL);
            System.out.println("Dropped existing table (if any).");

            // Create the table
            String createTableSQL = "CREATE TABLE student_grades (" +
                    "student_id STRING, " +
                    "course_id STRING, " +
                    "roll_no STRING, " +
                    "email_id STRING, " +
                    "grade STRING" +
                    ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
            jdbcTemplate.execute(createTableSQL);
            System.out.println("Created new table.");

            // Skip header row
            String[] headers = reader.readNext();
            if (headers == null) {
                System.out.println("CSV file is empty.");
                return;
            }

            // Write the remaining rows to the new temporary CSV file
            String[] line;
            int rowCount = 0;
            while ((line = reader.readNext()) != null) {
                writer.write(String.join(",", line));
                writer.newLine();
                rowCount++;
            }

            // Load data from the new CSV (without header)
            String loadDataSQL = "LOAD DATA LOCAL INPATH '/opt/hive/mydata/student_grades_noheader.csv' INTO TABLE student_grades";
            jdbcTemplate.execute(loadDataSQL);
            System.out.println("Loaded CSV data into Hive.");

            // Clean up temporary file
            Files.deleteIfExists(tempCsvFile);

            System.out.println("Inserted " + rowCount + " rows into Hive.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
