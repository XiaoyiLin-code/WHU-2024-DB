package edu.whu.tmdb.query.operations.utils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import com.alibaba.fastjson2.JSON;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemConnect {
    // 进行内存操作的一些一些方法和数据
    private static Logger logger = LoggerFactory.getLogger(MemConnect.class);
    private MemManager memManager;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // 1. 私有静态变量，用于保存MemConnect的单一实例
    private static volatile MemConnect instance = null;

    // 2. 私有构造函数，确保不能从类外部实例化
    private MemConnect() {
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }
    }

    // 3. 提供一个全局访问点
    public static MemConnect getInstance(MemManager mem) {
        // 双重检查锁定模式
        if (instance == null) { // 第一次检查
            synchronized (MemConnect.class) {
                if (instance == null) { // 第二次检查
                    instance = new MemConnect(mem);
                }
            }
        }
        return instance;
    }

    private MemConnect(MemManager mem) { this.memManager = mem; }

    //获取tuple
    public Tuple GetTuple(int id) {
        rwLock.readLock().lock(); // 获取读锁
        Tuple t = null;
        try {
            Object searchResult = this.memManager.search(new K("t" + id));
            if (searchResult == null)
                t= null;
            if (searchResult instanceof Tuple)
                t = (Tuple) searchResult;
            else if (searchResult instanceof V)
                t= JSON.parseObject(((V) searchResult).valueString, Tuple.class);
            if (t.delete)
                t= null;
        }finally {
            rwLock.readLock().unlock();
            return t;
        }
    }

    //插入tuple
    public void InsertTuple(Tuple tuple) {
        rwLock.writeLock().lock(); // 获取写锁
        try {
            this.memManager.add(tuple);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    //删除tuple
    public void DeleteTuple(int id) {
        rwLock.writeLock().lock();
        try {
            if (id >= 0) {
                Tuple tuple = new Tuple();
                tuple.tupleId = id;
                tuple.delete = true;
                memManager.add(tuple);
            }
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    // 更新tuple
    public void UpateTuple(Tuple tuple, int tupleId) {
        rwLock.writeLock().lock();
        try {
            tuple.tupleId = tupleId;
            this.memManager.add(tuple);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 给定表名(类名), 获取表在classTable中的id值
     * @param tableName 表名(类名)
     * @return 给定表名(类名)所对应的class id
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public int getClassId(String tableName) throws TMDBException {
        for (ClassTableItem item : getClassTableList()) {
            if (item.classname.equals(tableName)) {
                return item.classid; // 找到匹配的表名，返回对应的类ID
            }
        }
        // 如果遍历完成后没有找到匹配的表名，抛出(-1): tjoin deputy class 需要的，而非直接报错
        //throw new TMDBException(ErrorList.CLASS_NAME_DOES_NOT_EXIST, "No class found with the name: " + tableName);
        return -1;
    }

    /**
     * 给定表名(类名), 获取表在classTable中的属性名列表
     * @param tableName 表名(类名)
     * @return 给定表名(类名)所对应的属性名列表
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public List<String> getColumns(String tableName) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList(); // Assuming this method exists and returns the list of all class table entries
        List<String> attributes = new ArrayList<>();
        for (ClassTableItem item : classTableItems) {
            if (item.classname.equals(tableName)) {
                attributes.add(item.attrname);
            }
        }
        if (attributes.isEmpty())
            // Assuming each ClassTableItem has a list of attribute names stored in `attributes`
            // If no class is found with the provided name, throw an exception
            throw new TMDBException(ErrorList.CLASS_NAME_DOES_NOT_EXIST, "No class found with the name: " + tableName);
        else
            return attributes;
    }

    /**
     * Given a classId, get the attribute names of the class
     * @param classId The id of the class
     * @return The attribute names of the given classId
     * @throws TMDBException If no class is found with the given id, throw an exception
     */
    public List<String> getColumns(int classId) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList(); // Assuming this method returns a list of all class table entries
        List<String> attributes = new ArrayList<>();
        for (ClassTableItem item : classTableItems) {
            if (item.classid == classId) {
                attributes.add(item.attrname);
            }
        }
        if (attributes.isEmpty())
            // If no class is found with the provided ID, throw an exception
            throw new TMDBException(ErrorList.CLASS_ID_DOES_NOT_EXIST,"No class found with ID: " + classId);
        else
            return attributes;
    }


    /**
     * 给定表名(类名), 获取表在中属性的数量
     * @param tableName 表名(类名)
     * @return 给定表名(类名)所具有的属性数量(attrNum)
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public int getClassAttrnum(String tableName) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList();
        for (ClassTableItem item : classTableItems) {
            if (item.classname.equals(tableName)) {
                return item.attrnum;
            }
        }
        throw new TMDBException(ErrorList.CLASS_NAME_DOES_NOT_EXIST, "No class found with the name: " + tableName);
    }

    /**
     * 给定表id(类id), 获取表在classTable中的属性数量
     * @param classId 表名(类名)
     * @return 给定表名(类名)所具有的属性数量(attrNum)
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public int getClassAttrnum(int classId) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList(); // Assuming this method exists and returns a list of all class table entries
        for (ClassTableItem item : classTableItems) {
            if (item.classid == classId) {
                return item.attrnum; // Return the number of attributes for the class
            }
        }
        // If no class is found with the provided ID, throw an exception
        throw new TMDBException(ErrorList.CLASS_ID_DOES_NOT_EXIST,"No class found with ID: " + classId);
    }

    /**
     * 用于获取插入位置对应的属性id列表 (attrid)
     * @param classId insert对应的表id/类id
     * @param columns insert对应的属性名称列表
     * @return 属性名列表对应的attrid列表
     */
    public int[] getAttridList(int classId, List<String> columns) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList(); // Assuming this method exists and returns a list of all class table entries
        List<Integer> id = new ArrayList<>();

        for (ClassTableItem item : classTableItems) {
            if (item.classid == classId && columns.contains(item.attrname) ) {
                id.add(item.attrid);
            }
        }
        if(id.isEmpty())
            // If no class is found with the provided ID, throw an exception
            throw new TMDBException(ErrorList.CLASS_ID_DOES_NOT_EXIST,"No class found with ID: " + classId);
        int[] intArray = id.stream().mapToInt(Integer::intValue).toArray();
        return  intArray;

    }

    /**
     * 给定classId和attrName，返回属性的attrid
     * @param classId 类id号
     * @param attrName 属性名称
     * @return 属性对应的id
     */
    public int getAttrid(int classId, String attrName) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList(); // Assuming this method exists and returns a list of all class table entries
        for (ClassTableItem item : classTableItems) {
            if (item.classid == classId && attrName.equals(item.attrname) ) {
                return item.attrid;
            }
        }
        return -1;
    }

    /**
     * 给定表名，获取表下的所有元组
     * @param fromItem 表名
     * @return 查询语句中，该表之下所具有的所有元组
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public TupleList getTupleList(FromItem fromItem) throws TMDBException {
        int classId = getClassId(((Table) fromItem).getName());
        TupleList tupleList = new TupleList();
        for (ObjectTableItem item : getObjectTableList()) {
            if (item.classid != classId) {
                continue;
            }
            Tuple tuple = GetTuple(item.tupleid);
            if (tuple != null && !tuple.delete) {
                tuple.setTupleId(item.tupleid);
                tupleList.addTuple(tuple);
            }
        }
        return tupleList;
    }

    /**
     * 给定classID，获取表下的所有元组
     * @param classId 类id
     * @return 查询语句中，该表之下所具有的所有元组
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public TupleList getTupleList(int classId) throws TMDBException {
        TupleList tupleList = new TupleList();
        for (ObjectTableItem item : getObjectTableList()) {
            if (item.classid != classId) {
                continue;
            }
            Tuple tuple = GetTuple(item.tupleid);
            if (tuple != null && !tuple.delete) {
                tuple.setTupleId(item.tupleid);
                tupleList.addTuple(tuple);
            }
        }
        return tupleList;
    }

    /**
     * 给定表名，获取表名class table的副本
     * @param fromItem 表名
     * @return 表名对应的class table副本
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public ArrayList<ClassTableItem> copyClassTableList(FromItem fromItem) throws TMDBException{
        ArrayList<ClassTableItem> classTableList = new ArrayList<>();
        for (ClassTableItem item : getClassTableList()){
            if (item.classname.equals(((Table)fromItem).getName())){
                // 硬拷贝，不然后续操作会影响原始信息
                ClassTableItem classTableItem = item.getCopy();
                if (fromItem.getAlias() != null) {
                    classTableItem.alias = fromItem.getAlias().getName();
                }
                classTableList.add(classTableItem);
            }
        }
        if (classTableList.isEmpty()) {
            throw new TMDBException(ErrorList.CLASS_NAME_DOES_NOT_EXIST, ((Table)fromItem).getName());
        }
        return classTableList;
    }

    /**
     * 给定表名，返回该表是否存在，存在返回true
     * @param tableName 表名
     * @return 存在返回true，否则返回false
     */
    public boolean classExist(String tableName) {
        List<ClassTableItem> classTableItems = getClassTableList(); // 获取所有类信息的列表
        for (ClassTableItem item : classTableItems) {
            if (item.classname.equals(tableName)) {
                return true; // 如果找到匹配的类名，返回true
            }
        }
        return false; // 如果遍历完列表后没有找到，返回false
    }

    /**
     * 给定表名和属性名，返回该属性是否存在，存在返回true
     * @param tableName 表名
     * @param columnName 属性名
     * @return 存在返回true，否则返回false
     */
    public boolean columnExist(String tableName, String columnName) throws TMDBException {
        List<ClassTableItem> classTableItems = getClassTableList(); // 获取所有类信息的列表
        for (ClassTableItem item : classTableItems) {
            if (item.classname.equals(tableName)&&item.attrname.equals(columnName)) {
                return true; // 如果找到匹配的类名，返回true
            }
        }
        return false; // 如果遍历完列表后没有找到，返回false
    }

    /**
     * 给定class id, 获取该源类对应的所有代理类id
     * @param classId 源类的class id
     * @return 该class id对应的所有代理类id
     * @throws TMDBException 不存在给定表名的表，抛出异常
     */
    public ArrayList<Integer> getDeputyIdList(int classId) throws TMDBException {
        List<DeputyTableItem> deputyTableItems = getDeputyTableList(); // Assuming this method returns a list of all deputy table entries
        ArrayList<Integer> deputyIds = new ArrayList<>();
        boolean found = false;
        for (DeputyTableItem item : deputyTableItems) {
            if (item.originid == classId) {
                deputyIds.add(item.deputyid);
                found = true;
            }
        }
        return deputyIds;
    }

    /**
     * Given a deputyClassId, get the corresponding origin classes
     * @param deputyClassId The id of the deputy class
     * @return The origin classes of the given deputyClassId
     * @throws TMDBException If no deputy class is found with the given id, throw an exception
     */
    public List<Integer> getOriginIDList(int deputyClassId) throws TMDBException {
        List<DeputyTableItem> deputyTableItems = getDeputyTableList(); // Assuming this method returns a list of all deputy table entries
        List<Integer> originClasses = new ArrayList<>();
        for (DeputyTableItem item : deputyTableItems) {
            if (item.deputyid == deputyClassId) {
                originClasses.add(item.originid); // Add the origin class for the deputy class
            }
        }
        if (originClasses.isEmpty())
            // If no deputy class is found with the provided ID, throw an exception
            throw new TMDBException(ErrorList.DEPUTY_ID_DOES_NOT_EXIST,"No deputy class found with ID: " + deputyClassId);
        else
            return originClasses;
    }

    /**
     * Given an originId and a deputyId, get the corresponding deputyId
     * @param originId1 The id of the origin class
     * @param deputyId The id of the deputy class
     * @return The deputy class of the given originId
     * @throws TMDBException If no deputy class is found with the given id, throw an exception
     */
    public Integer getAnotherOriginID(int deputyId, int originId1) throws TMDBException {
        List<DeputyTableItem> deputyTableItems = getDeputyTableList(); // Assuming this method returns a list of all deputy table entries
        for (DeputyTableItem item : deputyTableItems) {
            if (item.deputyid == deputyId && item.originid != originId1) {
                return item.originid; // Return the origin class for the deputy class
            }
        }
        // If no deputy class is found with the provided ID, throw an exception
        throw new TMDBException(ErrorList.DEPUTY_ID_DOES_NOT_EXIST,"No deputy class found with ID: " + deputyId);
    }

    /**
     * Given a deputyId, get the corresponding deputytype
     * @param deputyId The id of the deputy class
     * @return The deputytype of the given deputyId
     * @throws TMDBException If no deputy class is found with the given id, throw an exception
     */
    public String[] getDeputyType(int deputyId) throws TMDBException {
        List<DeputyTableItem> deputyTableItems = getDeputyTableList(); // Assuming this method returns a list of all deputy table entries
        for (DeputyTableItem item : deputyTableItems) {
            if (item.deputyid == deputyId) {
                return item.deputyrule; // Return the deputytype for the deputy class
            }
        }
        // If no deputy class is found with the provided ID, throw an exception
        throw new TMDBException(ErrorList.DEPUTY_ID_DOES_NOT_EXIST,"No deputy class found with ID: " + deputyId);
    }

    /**
     * Given an originId, get the corresponding deputytypes
     * @param originId The id of the origin class
     * @return The deputytypes of the given originId
     * @throws TMDBException If no deputy class is found with the given id, throw an exception
     */
    public String[][] getDeputyTypeList(int originId) throws TMDBException {
        List<DeputyTableItem> deputyTableItems = getDeputyTableList(); // Assuming this method returns a list of all deputy table entries
        List<String[]> deputyTypes = new ArrayList<>();
        for (DeputyTableItem item : deputyTableItems) {
            if (item.originid == originId) {
                deputyTypes.add(item.deputyrule); // Add the deputytype for the deputy class
            }
        }
        return deputyTypes.toArray(new String[0][]);
    }

    public boolean Condition(String attrtype, Tuple tuple, int attrid, String value1) {
        String value = value1.replace("\"", "");
        switch (attrtype) {
            case "int":
                int value_int = Integer.parseInt(value);
                if (Integer.parseInt((String) tuple.tuple[attrid]) == value_int)
                    return true;
                break;
            case "char":
                String value_string = value;
                if (tuple.tuple[attrid].equals(value_string))
                    return true;
                break;

        }
        return false;
    }

    public void SaveAll() { memManager.saveAll(); }

    public void reload() {
        try {
            memManager.loadClassTable();
            memManager.loadDeputyTable();
            memManager.loadBiPointerTable();
            memManager.loadSwitchingTable();
        }catch (IOException e){
            logger.error(e.getMessage());
        }
    }

    public static class OandB {
        public List<ObjectTableItem> o = new ArrayList<>();
        public List<BiPointerTableItem> b = new ArrayList<>();

        public OandB() {
        }

        public OandB(MemConnect.OandB oandB) {
            this.o = oandB.o;
            this.b = oandB.b;
        }

        public OandB(List<ObjectTableItem> o, List<BiPointerTableItem> b) {
            this.o = o;
            this.b = b;
        }
    }

    // 获取系统表
    public static ObjectTable getObjectTable() { return MemManager.objectTable; }

    public static ClassTable getClassTable() { return MemManager.classTable; }

    public static DeputyTable getDeputyTable() { return MemManager.deputyTable; }

    public static BiPointerTable getBiPointerTable() { return MemManager.biPointerTable; }

    public static SwitchingTable getSwitchingTable() { return MemManager.switchingTable; }

    // 获取系统表表项
    public static List<ObjectTableItem> getObjectTableList() { return MemManager.objectTable.objectTableList; }

    //通过元组id获取ObjectTableItem
    public static List<ObjectTableItem> getObjectTableItemByTuple(Integer tupleid) {
        List<ObjectTableItem> objectTableItems = new ArrayList<>();
            for (ObjectTableItem item : getObjectTableList()) {
                if (item.tupleid == tupleid) {
                    objectTableItems.add(item);
                }
        }
        return objectTableItems;
    }

    public static List<ClassTableItem> getClassTableList() { return MemManager.classTable.classTableList; }

    public static List<DeputyTableItem> getDeputyTableList() { return MemManager.deputyTable.deputyTableList; }

    public static List<BiPointerTableItem> getBiPointerTableList() { return MemManager.biPointerTable.biPointerTableList; }

    public static List<SwitchingTableItem> getSwitchingTableList() { return MemManager.switchingTable.switchingTableList; }
}