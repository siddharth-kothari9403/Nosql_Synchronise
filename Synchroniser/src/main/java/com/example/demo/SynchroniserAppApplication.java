package com.example.demo;

import com.example.demo.ImportCSVs.CsvToMongoImporter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SynchroniserAppApplication {

	public static void main(String[] args) {
		CsvToMongoImporter.importFile();
		SpringApplication.run(SynchroniserAppApplication.class, args);
	}

}
