package edu.whu.tmdb;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.util.DbOperation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static edu.whu.tmdb.util.DbOperation.*;
import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;


/*test methods*/
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTable;

public class Main {
    public static void main(String[] args) throws IOException, TMDBException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String sqlCommand;
        System.out.println("1");
        if (args.length > 0) {
            switch (args[0]) {
                case "test1":
                    test1();

                    break;
                case "test2":
                    test2();
                    break;
                default:
                    System.out.println("Invalid command.");
                    break;
            }
        } else {
            System.out.println("No arguments provided.");
        }

        // 调试用
        while (true) {
            System.out.print("tmdb> ");
            sqlCommand = reader.readLine().trim();
            sqlCommand = reader.readLine().trim();
            /*For debug*/
            System.out.println("Running:"+sqlCommand);
            /*For debug end*/
            if ("exit".equalsIgnoreCase(sqlCommand)) {
                break;
            } else if ("resetdb".equalsIgnoreCase(sqlCommand)) {
                DbOperation.resetDB();
            } else if ("show BiPointerTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showBiPointerTable();
            } else if ("show ClassTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showClassTable();
            } else if ("show DeputyTable".equalsIgnoreCase(sqlCommand)) {;
                DbOperation.showDeputyTable();
            } else if ("show SwitchingTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showSwitchingTable();
            } else if (!sqlCommand.isEmpty()) {
                SelectResult result = execute(sqlCommand);
                if (result != null) {
                    DbOperation.printResult(result);
                }
            }
        }


        // execute("show tables;");
        // execute(args[0]);
        // transaction.test();
        // transaction.test2();
        // insertIntoTrajTable();
        // testMapMatching();
        // testEngine();
        // testTorch3();
    }

    private static void test1() throws TMDBException {
        System.out.println("Running test1...");
        MemManager memManager = new MemManager();
        MemConnect memConnect = MemConnect.getInstance(memManager);
        testGetClassId(memConnect, "test");  // 用实际的类名替换 "YourClassNameHere"
        // Test 1 logic
        System.out.println("Running test1.1...");
        List attrname= memConnect.getColumns("test");  // 用实际的类名替换 "YourClassNameHer
        System.out.println("attrname:"+attrname);
        // Test 1.1 logic
        System.out.println("Running test1.2...");
        int attrnum=memConnect.getClassAttrnum("test");
        System.out.println("attrnum:"+attrnum);
        // Test 1.2 logic

        System.out.println("Running test1.3...");
        int attrnum1=memConnect.getClassAttrnum(1);
        System.out.println("attrnum:"+attrnum1);
        // Test 1.3 logic

        System.out.println("Running test1.4...");
        List<String> fixedList = Collections.unmodifiableList(Arrays.asList("id_1", "value_1"));
        int[] ids= memConnect.getAttridList(1, fixedList);
        System.out.println("ids array: " + Arrays.toString(ids));
        // Test 1.4 logic

        System.out.println("Running test1.5...");
        int id= memConnect.getAttrid(1, "id_1");
        System.out.println("id: " + id);
        // Test 1.5 logic

        System.out.println("Running test1.6...");
        boolean class_exist = memConnect.classExist("table1");
        System.out.println("class_exist: " + class_exist);
        // Test 1.6 logic

        System.out.println("Running test1.7...");
        boolean column_exist = memConnect.columnExist("table1","value_1");
        System.out.println("column_exist: " + column_exist);
        // Test 1.7 logic

        System.out.println("Running test1.8...");
        List<Integer> duex = memConnect.getDeputyIdList(1);
        System.out.println("column_exist: " + duex);
        // Test 1.8 logic
    }

    private static void test2() {

    }

    private static void test3() {
        // Test 1 logic
    }
    public static void insertIntoTrajTable(){
        Transaction transaction = Transaction.getInstance();
        transaction.insertIntoTrajTable();
        transaction.SaveAll();
    }

    public static void testEngine() throws IOException {
        Transaction transaction = Transaction.getInstance();
        transaction.testEngine();
    }

    public static void testMapMatching() {
        Transaction transaction = Transaction.getInstance();
        transaction.testMapMatching();
    }


    public static void testGetClassId(MemConnect memConnect, String className) {
        try {
            int classId = memConnect.getClassId(className);
            System.out.println("Class ID for " + className + " is: " + classId);
        } catch (TMDBException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }


    public static SelectResult execute(String s)  {
        Transaction transaction = Transaction.getInstance();    // 创建一个事务实例
        SelectResult selectResult = null;
        try {
            // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);
            selectResult = transaction.query("", -1, stmt);
            if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
                transaction.SaveAll();
            }
        }catch (JSQLParserException e) {
            // e.printStackTrace();    // 打印语法错误的堆栈信息
            System.out.println("syntax error");
        }
        return selectResult;
    }

}