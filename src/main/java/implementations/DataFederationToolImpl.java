package implementations;

import au.com.bytecode.opencsv.CSVReader;
import interfaces.ResultCollector;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

/**
 * Nástroj provede inner join Derby databáze a daného csv souboru, program
 * počítá s tím že databáze je uložena na serveru //localhost:1527/FedSourceA,
 * podle potřeby jde i upravit
 *
 * @author Jan Kasztura, 422563
 * @version 13.4.2015
 */
public class DataFederationToolImpl {

    /**
     * Metoda nejprve načte seřazenou databázi, potom seřadí quicksortem csv
     * soubor. Potom postupně prochází jednotlivé databáze řádek po řádku, pokud
     * je klíč řádku databáze nižší než klíč řádku csv souboru, přesune se na
     * další. Podobně procházíme u csv soubru. Pokud se klíče shodují, pošle
     * spojené řádky resultCollectoru. Data z Derby databáze jsou z tabulky TEST
     *
     * Složitost je O( n*log(n) + m*log(m) + m + n ) kde m je počet řádků derby
     * DB a n je počet řádků v csv
     *
     * @param csvFile soubor csv
     * @param databaseColumn index sloupce, kde je primární klíč
     * @param csvColumn index sloupce kde je cizí klíč
     * @param resultCollector
     * @throws Exception pokd se nepodaří připojit na databázi nebo nastal
     * nějaký jiný problém
     */
    public void federate(File csvFile, int databaseColumn, int csvColumn, ResultCollector resultCollector) throws Exception {
        Connection conn;
        try {
            conn = DriverManager.getConnection("jdbc:derby://localhost:1527/FedSourceA");
        } catch (SQLException ex) {
            throw new Exception(ex);
        }
        DatabaseMetaData md = conn.getMetaData();
        ResultSet columnInfo = md.getColumns(null, null, "TEST", "%");
        for (int i = 0; i < databaseColumn; i++) {
            boolean shouldExecute = columnInfo.next();
            if (!shouldExecute) {
                throw new Exception("Not enought columns in derby DB");
            }
        }
        String columnName = columnInfo.getString(4);
        PreparedStatement st;
        try {
            String SQL = "SELECT * FROM TEST ORDER BY %s";
            st = conn.prepareStatement(String.format(SQL, columnName));
        } catch (SQLException ex) {
            throw ex;
        }

        ResultSet resultSet = st.executeQuery();
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader(csvFile));
        } catch (FileNotFoundException ex) {
            throw new Exception(ex);
        }
        List<String[]> file = reader.readAll();
        if (file.isEmpty() || file.get(0).length < csvColumn) {
            throw new Exception("Not enought columns in CSV file");
        }
        myQuicksort(file, 0, file.size(), csvColumn - 1);

        int csvIndex = 0;
        String[] fileRow = file.get(csvIndex++);
        resultSet.next();
        boolean execute = true;
        while (execute) {
            while (resultSet.getString(databaseColumn).compareTo(fileRow[csvColumn - 1]) == 0) {
                List<String> result = new ArrayList<>();
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    result.add(resultSet.getString(i));
                }
                result.addAll(Arrays.asList(fileRow));
                resultCollector.collectRow(result);
                if (csvIndex < file.size()) {
                    fileRow = file.get(csvIndex++);
                } else {
                    execute = false;
                    break;
                }
            }
            if (resultSet.getString(databaseColumn).compareTo(fileRow[csvColumn - 1]) > 0) {
                if (csvIndex < file.size()) {
                    fileRow = file.get(csvIndex++);
                }
            } else {
                if (!resultSet.next()) {
                    execute = false;
                }
            }
        }
    }

    /**
     * Quicksort funkce k seřazení souboru podle daného sloupce
     *
     * @param data
     * @param left index elementu od kterého řadíme
     * @param right index elementu, ke kterému už nemůžeme přistoupit
     * @param column sloupec, podle kterého řadíme
     */
    private static void myQuicksort(List<String[]> data, int left, int right, int column) {
        if (left < right) {
            int boundary = left;
            for (int i = left + 1; i < right; i++) {
                if (data.get(i)[column].compareTo(data.get(left)[column]) < 0) {
                    swap(data, i, ++boundary);
                }
            }
            swap(data, left, boundary);
            myQuicksort(data, left, boundary, column);
            myQuicksort(data, boundary + 1, right, column);
        }
    }

    /**
     * Metoda k prohození dvou elementů
     *
     * @param array
     * @param left
     * @param right
     */
    private static void swap(List<String[]> array, int left, int right) {
        Collections.swap(array, right, left);
    }
}
