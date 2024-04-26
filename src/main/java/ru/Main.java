package ru;

/* Повторить все, что было на семинаре на таблице Student с полями
1. id - bigint
2. first_name - varchar(256)
3. second_name - varchar(256)
4. group - varchar(128)

Написать запросы:
1. Создать таблицу
2. Наполнить таблицу данными (insert)
3. Поиск всех студентов
4. Поиск всех студентов по имени группы

Доп. задания:
1. ** Создать таблицу group(id, name); в таблице student сделать внешний ключ на group
2. ** Все идентификаторы превратить в UUID

Замечание: можно использовать ЛЮБУЮ базу данных: h2, postgres, mysql, ...*/

import java.sql.*;
import java.util.UUID;

public class Main {
    private static final String URL = "jdbc:mysql://localhost:3306/GB_students";
    private static final String USER = "root";
    private static final String PASSWORD = "12345678";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(URL,USER, PASSWORD)){
//        createSchema(connection);
//        createTableGroup(connection);
//        insertDataGroup(connection);
//        createTable(connection);
//        insertData(connection);
//        selectStudents(connection);
            selectStudentsByGroup(connection,"group1");

        } catch (SQLException e) {
            System.err.println("Соединение не удалось " + e.getMessage());
        }
    }
    private static void createSchema(Connection connection){
        try (Statement statement = connection.createStatement()){
            statement.execute("DROP SCHEMA GB_students;");
            statement.execute("CREATE SCHEMA GB_students;");

        } catch (SQLException e) {
            System.err.println("База данных не создана" + e);
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE `GB_students`.`students`(
                    `id` VARCHAR(36) PRIMARY KEY,
                    `first_name` VARCHAR(256) NOT NULL,
                    `second_name` VARCHAR(256) NOT NULL,
                    `group_id` VARCHAR(128) NOT NULL,
                    FOREIGN KEY (group_id)
                       REFERENCES `groups` (id)
                    );
                    """);
        }
    }
    private static void insertData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO `GB_students`.`students`(`id`,`first_name`, `second_name`,`group_id`) VALUES" +
                    "(UUID(), 'Ivan', 'Ivanov','" + getIdGroup(connection,"group1")+ "')," +
                    "(UUID(), 'Petr', 'Petrov','" + getIdGroup(connection,"group2")+ "')," +
                    "(UUID(), 'Kolya', 'Kolyanov','" + getIdGroup(connection,"group1")+ "');");
        }
    }
    private static void createTableGroup(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE `GB_students`.`groups`(
                    `id` VARCHAR(36) PRIMARY KEY,
                    `group_name` VARCHAR(128) NOT NULL);
                    """);
        }
    }
    private static void insertDataGroup(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    INSERT INTO `GB_students`.`groups`(`id`,`group_name`)
                    VALUES
                    (UUID(),'group1'),
                    (UUID(), 'group2'),
                    (UUID(), 'group3');
                    """);
        }
    }
    static String getIdGroup(Connection connection, String group_n) throws SQLException{
        String id= null;
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select id
                    from `GB_students`.`groups`
                    WHERE `group_name` = '""" + group_n + "';");
            if (resultSet.next()){
                id =  resultSet.getString("id");
            }
        }
        return id;
    }

    private static void selectStudents(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    SELECT s.`id`,`first_name`, `second_name`,`group_id`
                    FROM `GB_students`.`students` s
                    INNER JOIN `GB_students`.`groups` g ON s.`group_id` = g.`id`;
                    """);
            while (resultSet.next()){
                UUID id = UUID.fromString(resultSet.getString("id"));
                String first_name = resultSet.getString("first_name");
                String second_name = resultSet.getString("second_name");
                UUID group_id = UUID.fromString(resultSet.getString("group_id"));
                System.out.printf("Результат поиска: %s, %s, %s, %s \n", id,first_name, second_name, group_id );
            }
        }
    }
    private static void selectStudentsByGroup(Connection connection, String numberGroup) throws SQLException {
        String str = """
                    SELECT s.`id`,`first_name`, `second_name`,`group_id`
                    FROM `GB_students`.`students` s
                    INNER JOIN `GB_students`.`groups` g ON s.`group_id` = g.`id`
                    WHERE `group_id` =?
                    """;
        try (PreparedStatement statement = connection.prepareStatement(str)) {
            statement.setString(1,numberGroup);
            ResultSet resultSet = statement.executeQuery("""
                    SELECT s.`id`,`first_name`, `second_name`,`group_id`
                    FROM `students` s
                    INNER JOIN `groups` g ON s.`group_id` = g.`id`
                    WHERE `group_id` ='"""+ getIdGroup(connection,numberGroup) + "';");
            while (resultSet.next()){
                UUID id = UUID.fromString(resultSet.getString("id"));
                String first_name = resultSet.getString("first_name");
                String second_name = resultSet.getString("second_name");
                UUID group_id = UUID.fromString(resultSet.getString("group_id"));
                System.out.printf("Результат поиска: %s, %s, %s, %s \n", id,first_name, second_name, group_id );
            }
        }
    }
}