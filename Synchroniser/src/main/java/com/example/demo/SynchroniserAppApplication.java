package com.example.demo;

import com.example.demo.ImportCSVs.CsvToHiveImporter;
import com.example.demo.ImportCSVs.CsvToMongoImporter;
import com.example.demo.ImportCSVs.CsvToPostgresImporter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SynchroniserAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(SynchroniserAppApplication.class, args);
	}

	@Bean
	public CommandLineRunner run(CsvToMongoImporter csvToMongoImporter, CsvToPostgresImporter csvToPostgresImporter) {
		return args -> {
			csvToMongoImporter.importFile();
			csvToPostgresImporter.importFile();
		};
	}

}
