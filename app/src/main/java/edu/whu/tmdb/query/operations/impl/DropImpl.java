package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.utils.MemConnect;

public class DropImpl implements Drop {

    private MemConnect memConnect;

    public DropImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String tableName = drop.getName().getName();
        int classId = memConnect.getClassId(tableName);
        drop(classId);
        return true;
    }

    public void drop(int classId) {
        // TODO-task4
        //首先删除自身类
        dropClassTable(classId);                            // 1.删除ClassTableItem
        // 接下来删除相关的代理类
        ArrayList<Integer> deputyClassIdList = new ArrayList<>();   // 存储该类对应所有代理类id
        dropDeputyClassTable(classId, deputyClassIdList);   // 2.在DeputyTable表中获取代理类id并删除DeputyTableItem
        dropBiPointerTable(classId);                        // 3.删除BiPointerTable
        dropSwitchingTable(classId);                        // 4.删除switchingTable
        dropObjectTable(classId);                           // 5.删除已创建的源类对象

        // 6.递归删除代理类相关
        // TODO-task4
    }

    /**
     * 给定要删除的class id，删除系统表类表(class table)中的表项
     * @param classId 要删除的表对应的id
     */
    private void dropClassTable(int classId) {
        List<ClassTableItem> classTableList = MemConnect.getClassTableList();

        // Remove all items from the class table that match the given classId
        boolean removed = classTableList.removeIf(item -> item.classid == classId);

        // Optionally, handle the case where no items were found (and thus nothing was removed)
        if (!removed) {
            System.out.println("No entries found for classId: " + classId);
        }
    }

    /**
     * 删除系统表中的deputy table，并获取class id对应源类的代理类id
     * @param classId 源类id
     * @param deputyClassIdList 作为返回值，源类对应的代理类id列表
     */
    private void dropDeputyClassTable(int classId, ArrayList<Integer> deputyClassIdList) {
        // Access the list of all deputy class table items
        List<DeputyTableItem> deputyTableList = MemConnect.getDeputyTableList();

        // Use an iterator to safely remove items while iterating
        Iterator<DeputyTableItem> iterator = deputyTableList.iterator();
        while (iterator.hasNext()) {
            DeputyTableItem item = iterator.next();
            if (item.originid == classId) {
                // Add the deputy class ID to the list before removing the item
                deputyClassIdList.add(item.deputyid);
                iterator.remove();  // Remove the item from the list
            }
            else if (item.deputyid == classId) {
                iterator.remove();  // Remove the item from the list
            }
        }

        // Optionally, check if any deputies were found and removed
        if (deputyClassIdList.isEmpty()) {
            System.out.println("No deputy classes found for classId: " + classId);
        } else {
            for(Integer deputyid:deputyClassIdList) {
                MemConnect.getClassTableList().removeIf(item -> item.classid == deputyid);}
            System.out.println("Deputy class entries removed for classId: " + classId + ", Deputy IDs: " + deputyClassIdList);
        }
    }


    /**
     * 删除系统表中的BiPointerTable
     * @param classId 源类id
     */
    private void dropBiPointerTable(int classId) {
        // Attempt to remove entries from the bi-pointer table that match the given classId
        boolean removed = MemConnect.getBiPointerTableList().removeIf(item -> item.classid == classId);
        MemConnect.getBiPointerTableList().removeIf(item -> item.deputyid == classId);

        // Check if any items were actually removed
        if (!removed) {
            System.out.println("No entries found for classId: " + classId + " in BiPointerTable.");
        } else {
            System.out.println("Entries for classId: " + classId + " have been successfully removed from BiPointerTable.");
        }
    }


    /**
     * 删除系统表中的SwitchingTable
     * @param classId 源类id
     */
    private void dropSwitchingTable(int classId) {
        // Access the list of all switching table items
        List<SwitchingTableItem> switchingTableList = MemConnect.getSwitchingTableList();

        // Use the removeIf method to remove all entries with the specified classId
        boolean removed = switchingTableList.removeIf(item -> item.oriId == classId);
        switchingTableList.removeIf(item -> item.deputyId == classId);

        // Optionally, handle the case where no items were found (and thus nothing was removed)
        if (!removed) {
            System.out.println("No entries found for classId: " + classId + " in the SwitchingTable.");
        } else {
            System.out.println("All entries for classId: " + classId + " have been successfully removed from the SwitchingTable.");
        }
    }


    /**
     * 删除源类具有的所有对象的列表
     * @param classId 源类id
     */
    private void dropObjectTable(int classId) {
        // Get the list of all object table items
        List<ObjectTableItem> objectTableList = MemConnect.getObjectTableList();

        // Use the removeIf method to remove all entries with the specified classId
        boolean removed = objectTableList.removeIf(item -> item.classid == classId);

        // Optionally, handle the case where no items were found (and thus nothing was removed)
        if (!removed) {
            System.out.println("No entries found for classId: " + classId + " in the ObjectTable.");
        } else {
            System.out.println("All entries for classId: " + classId + " have been successfully removed from the ObjectTable.");
        }
    }


}
