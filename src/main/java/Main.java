import javax.xml.parsers.*; // Парсинг XML
import org.w3c.dom.*;   // Классы для работы с DOM
import java.sql.*;// драйвер API для работы с БД
import java.io.*; // Классы для работы с файловой системой
import java.text.SimpleDateFormat; // Парсинг дат
import java.util.*; // Scanner

class Main {
    // Данные для подключения к БД
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/Plants";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "ZmeiYT12321";
    private static final String DataXmlAdd = "C:\\Users\\artem\\IdeaProjects\\Test-Task\\src\\main\\resources\\data";

    public static void main(String[] args) {
        // Подключение к базе
        try (Scanner sc = new Scanner(System.in);
             Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            System.out.println("База данных успешно подключена!");
            File dataDir = new File(DataXmlAdd);
            // Основной цикл
            while (true) {
                processFileSelection(conn, sc, dataDir);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Ошибка подключения к БД", e);
        }
    }

    // Обработка выбора
    private static void processFileSelection(Connection conn, Scanner sc, File dataDir) {
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            System.out.println("Директория не найдена: " + dataDir.getAbsolutePath());
            return;
        }

        File[] files = dataDir.listFiles();
        if (files == null || files.length == 0) {
            System.out.println("В директории нет файлов");
            return;
        }

        printFileList(files);
        File selectedFile = getUserSelectedFile(sc, files, dataDir);

        if (selectedFile != null) {
            try {
                processXmlFile(conn, selectedFile);
            } catch (Exception e) {
                System.err.println("Ошибка обработки файла: " + e.getMessage());
            }
        }
    }

    // Вывод списка доступных файлов
    private static void printFileList(File[] files) {
        System.out.println("\nСписок файлов для парсинга:");
        int fileCount = 0;
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                System.out.println(++fileCount + ") " + files[i].getName());
            }
        }
    }

    // Поиск файла, выбранного пользователем
    private static File getUserSelectedFile(Scanner sc, File[] files, File dataDir) {
        System.out.print("\nВведите номер файла (1-" + countFiles(files) + ")(0 - выход): ");
        try {
            int selectedNumber = Integer.parseInt(sc.nextLine().trim());
            if (selectedNumber == 0) {
                System.out.println("Программа завершена!");
                System.exit(0);
            }
            return findSelectedFile(files, selectedNumber, dataDir);
        } catch (NumberFormatException e) {
            System.out.println("Ошибка: введите число");
        }
        return null;
    }

    // Подсчёт кол-ва файлов
    private static int countFiles(File[] files) {
        int count = 0;
        for (File file : files) {
            if (file.isFile()) count++;
        }
        return count;
    }

    // Поиск файла по номеру, который выбрал пользователь
    private static File findSelectedFile(File[] files, int selectedNumber, File dataDir) {
        int currentNumber = 0;
        for (File file : files) {
            if (file.isFile()) {
                if (++currentNumber == selectedNumber) {
                    System.out.println("\nВы выбрали файл: " + file.getName());
                    return new File(dataDir, file.getName());
                }
            }
        }
        System.out.println("Ошибка: введен неверный номер файла");
        return null;
    }

    // Основная обработка XML файла
    private static void processXmlFile(Connection conn, File fileName) throws Exception {
        Document doc = parseXmlFile(fileName);
        Element catalog = doc.getDocumentElement();

        int catalogId = saveCatalogData(conn, catalog);
        savePlantsData(conn, catalog, catalogId);
    }

    // Парсинг XML в DOM
    private static Document parseXmlFile(File fileName) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(fileName);
    }

    // Сохранение данных каталога в БД и получение ID
    private static int saveCatalogData(Connection conn, Element catalog) throws Exception {
        String uuid = catalog.getAttribute("uuid");
        String date = catalog.getAttribute("date");
        String company = catalog.getAttribute("company");

        String sql = "INSERT INTO D_CAT_CATALOG (DELIVERY_DATE, COMPANY, UUID) VALUES (?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
            Timestamp timestamp = new Timestamp(dateFormat.parse(date).getTime());

            stmt.setTimestamp(1, timestamp);
            stmt.setString(2, company);
            stmt.setString(3, uuid);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            throw new SQLException("Не удалось получить ID каталога");
        }
    }

    // Сохранение данных о растениях из каталога
    private static void savePlantsData(Connection conn, Element catalog, int catalogId) throws SQLException {
        NodeList plants = catalog.getElementsByTagName("PLANT");
        String sql = "INSERT INTO F_CAT_PLANTS (COMMON, BOTANICAL, ZONE, LIGHT, PRICE, AVAILABILITY, CATALOG_ID) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < plants.getLength(); i++) {
                Node plantNode = plants.item(i);
                if (plantNode.getNodeType() == Node.ELEMENT_NODE) {
                    processSinglePlant((Element) plantNode, stmt, catalogId);
                }
            }
        }
    }

    // Обработка одного растения
    private static void processSinglePlant(Element plant, PreparedStatement stmt, int catalogId) throws SQLException {
        String common = getElementText(plant, "COMMON");
        String botanical = getElementText(plant, "BOTANICAL");
        String zoneStr = getElementText(plant, "ZONE");
        String light = getElementText(plant, "LIGHT");
        String priceStr = getElementText(plant, "PRICE");

        double price = Double.parseDouble(priceStr.substring(1));
        int availability = Integer.parseInt(getElementText(plant, "AVAILABILITY"));

        setPlantStatementParameters(stmt, common, botanical, zoneStr, light, price, availability, catalogId);
        stmt.executeUpdate();

        System.out.println("Добавлено растение: " + common);
    }

    // Параметры для PreparedStatement растения
    private static void setPlantStatementParameters(PreparedStatement stmt,
                                                    String common, String botanical, String zoneStr, String light,
                                                    double price, int availability, int catalogId) throws SQLException {

        stmt.setString(1, common);
        stmt.setString(2, botanical);
        stmt.setObject(3, parseZoneValue(zoneStr), Types.INTEGER);
        stmt.setString(4, light);
        stmt.setDouble(5, price);
        stmt.setInt(6, availability);
        stmt.setInt(7, catalogId);
    }

    // Парсинг зоны морозостойкости
    private static Integer parseZoneValue(String zoneStr) {
        try {
            if (zoneStr == null || zoneStr.trim().isEmpty()) return null;
            if (zoneStr.equalsIgnoreCase("Годичный")) return null;

            if (zoneStr.contains("-")) {
                zoneStr = zoneStr.split("-")[0].trim();
            }
            return Integer.parseInt(zoneStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // текст из XML элемента
    private static String getElementText(Element parent, String tagName) {
        NodeList nodes = parent.getElementsByTagName(tagName);
        return nodes.getLength() > 0 ? nodes.item(0).getTextContent().trim() : "";
    }
}
