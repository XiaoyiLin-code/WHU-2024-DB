package edu.whu.tmdb.util;


import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.memory.Tuple;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static edu.whu.tmdb.query.operations.utils.MemConnect.getClassTableList;

public class DbOperation {
    /**
     * 给定元组查询结果，输出查询表格
     * @param result 查询语句的查询结果
     */
    public static void printResult(SelectResult result) {
        // 输出表头信息
        StringBuilder tableHeader = new StringBuilder("|");
        for (int i = 0; i < result.getAttrname().length; i++) {
            tableHeader.append(String.format("%-20s", result.getClassName()[i] + "." + result.getAttrname()[i])).append("|");
        }
        System.out.println(tableHeader);

        // 输出元组信息
        for (Tuple tuple : result.getTpl().tuplelist) {
            StringBuilder data = new StringBuilder("|");
            for (int i = 0; i < tuple.tuple.length; i++) {
                data.append(String.format("%-20s", tuple.tuple[i].toString())).append("|");
            }
            System.out.println(data);
        }
    }

    /**
     * 删除数据库所有数据文件，即重置数据库
     */
    public static void resetDB() {
        // 仓库路径
        String repositoryPath = "D:\\WHU-2024-DB";

        // 子目录路径
        String sysPath = repositoryPath + File.separator + "data\\sys";
        String logPath = repositoryPath + File.separator + "data\\log";
        String levelPath = repositoryPath + File.separator + "data\\level";

        List<String> filePath = new ArrayList<>();
        filePath.add(sysPath);
        filePath.add(logPath);
        filePath.add(levelPath);

        // 遍历删除文件
        for (String path : filePath) {
            File directory = new File(path);

            // 检查目录是否存在
            if (!directory.exists()) {
                System.out.println("目录不存在：" + path);
                return;
            }

            // 获取目录中的所有文件
            File[] files = directory.listFiles();
            if (files == null) { continue; }
            for (File file : files) {
                // 删除文件
                if (file.delete()) {
                    System.out.println("已删除文件：" + file.getAbsolutePath());
                } else {
                    System.out.println("无法删除文件：" + file.getAbsolutePath());
                }
            }
        }
    }

    public static void showBiPointerTable() {
        List<BiPointerTableItem> BIitems = MemManager.biPointerTable.biPointerTableList;
        // 输出表头
        System.out.println("| Class ID | Object ID      | Deputy ID | Deputy Object ID |");
        System.out.println("|----------|----------------|-----------|------------------|");

        // 输出表格数据行
        for (BiPointerTableItem BIitem : BIitems) {
            System.out.printf("| %-9d | %-14d | %-9d | %-16d |\n",
                    BIitem.classid,
                    BIitem.objectid,
                    BIitem.deputyid,
                    BIitem.deputyobjectid
            );
        }
    }


    public static void showClassTable() {
        // 获取全部的 ClassTableItem 实例，假设 getClassTableList() 方法已实现并可用
        List<ClassTableItem> classTableItems = MemManager.classTable.classTableList;
        // 打印表头
        System.out.println("| Class Name       | Class ID | Attribute Name    | Attribute ID | Attribute Type |");
        System.out.println("|------------------|----------|-------------------|--------------|----------------|");

        // 遍历每个 ClassTableItem 实例，每个实例代表一个属性
        for (ClassTableItem classItem : classTableItems) {
            System.out.printf("| %-16s | %-8d | %-17s | %-12d | %-14s |\n",
                    classItem.classname,
                    classItem.classid,
                    classItem.attrname,
                    classItem.attrid,
                    classItem.attrtype);
        }
    }

    public static void showDeputyTable() {
        List<DeputyTableItem> deputyItems = MemManager.deputyTable.deputyTableList; // Assume access to a list of deputy table items
        // Print table header
        System.out.println("| Origin ID | Deputy ID | Deputy Rules                           |");
        System.out.println("|-----------|-----------|----------------------------------------|");

        // Iterate over each item in the list and print formatted output
        for (DeputyTableItem item : deputyItems) {
            String rules = String.join(", ", item.deputyrule); // Join all rules into a single string for display
            System.out.printf("| %-10d | %-10d | %-38s |\n",
                    item.originid,
                    item.deputyid,
                    rules);
        }
    }


    public static void showSwitchingTable() {
        List<SwitchingTableItem> switchingItems = MemManager.switchingTable.switchingTableList; // Assumed access to a list of switching table items
        // Print table header
        System.out.println("| Ori ID | Ori Attr ID | Ori Attr Name    | Deputy ID | Deputy Attr ID | Deputy Attr Name  | Rule       |");
        System.out.println("|--------|-------------|------------------|-----------|----------------|-------------------|------------|");

        // Iterate over each item in the list and print formatted output
        for (SwitchingTableItem item : switchingItems) {
            System.out.printf("| %-6d | %-11d | %-17s | %-9d | %-14d | %-14s | %-11s |\n",
                    item.oriId,
                    item.oriAttrid,
                    item.oriAttr,
                    item.deputyId,
                    item.deputyAttrId,
                    item.deputyAttr,
                    item.rule);
        }
    }
}