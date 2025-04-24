package com.example.demo;
import com.example.demo.DBRead.HiveSystem;
import com.example.demo.DBRead.MongoDBSystem;
import com.example.demo.DBRead.PostgreSQLSystem;
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
	public CommandLineRunner run(PostgreSQLSystem postgreSQLSystem, MongoDBSystem mongoDBSystem, HiveSystem hiveSystem, TestCaseExecutor testCaseExecutor) {
		return args -> {
			postgreSQLSystem.importFile();
			mongoDBSystem.importFile();
			hiveSystem.importFile();
		};
	}
}
