package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;

public class InsertImpl implements Insert {
    private MemConnect memConnect;

    ArrayList<Integer> tupleIdList = new ArrayList<>();

    public InsertImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public ArrayList<Integer> insert(Statement stmt) throws TMDBException, IOException {
        net.sf.jsqlparser.statement.insert.Insert insertStmt = (net.sf.jsqlparser.statement.insert.Insert) stmt;
        Table table = insertStmt.getTable();        // 解析insert对应的表
        List<String> attrNames = new ArrayList<>(); // 解析插入的字段名
        if (insertStmt.getColumns() == null){
            attrNames = memConnect.getColumns(table.getName());
        }
        else{
            int insertColSize = insertStmt.getColumns().size();
            for (int i = 0; i < insertColSize; i++) {
                attrNames.add(insertStmt.getColumns().get(i).getColumnName());
            }
        }

        // 对应含有子查询的插入语句
        SelectImpl select = new SelectImpl();
        SelectResult selectResult = select.select(insertStmt.getSelect());

        // tuplelist存储需要插入的tuple部分
        TupleList tupleList = selectResult.getTpl();
        execute(table.getName(), attrNames, tupleList);
        return tupleIdList;
    }

    /**
     * @param tableName 表名/类名
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     */
    public void execute(String tableName, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int classId = memConnect.getClassId(tableName);         // 类id
        int attrNum = memConnect.getClassAttrnum(tableName);    // 属性的数量
        int[] attrIdList = memConnect.getAttridList(classId, columns);         // 插入的属性对应的attrid列表
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
            }
            tupleIdList.add(insert(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     * @param classId 表/类id
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     */
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attrIdList = memConnect.getAttridList(classId, columns);
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
            }
            tupleIdList.add(insert(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     * @param classId 要插入的类id
     * @param columns 要插入类的属性名列表
     * @param tuple 要insert的元组tuple
     * @return 新插入元组的tuple id
     */
    public int execute(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attridList = memConnect.getAttridList(classId, columns);

        if (tuple.tuple.length != columns.size()){
            throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
        }
        int tupleId = insert(classId, columns, tuple, attrNum, attridList);
        tupleIdList.add(tupleId);
        return tupleId;
    }


    /**
     * @param classId 插入表/类对应的id
     * @param columns 表/类所具有的属性名列表（来自insert语句）
     * @param tuple 要插入的元组
     * @param attrNum 元组包含的属性数量（系统表中获取）
     * @param attrId 插入属性对应的attrId列表（根据insert的属性名，系统表中获取）
     * @return 新插入属性的tuple id
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int attrNum, int[] attrId) throws TMDBException, IOException {
        // 1.直接在对应类中插入tuple
        // 1.1 获取新插入元组的id
        SelectImpl select=new SelectImpl();
        int tupleid = MemConnect.getObjectTable().maxTupleId++;

        // 1.2 将tuple转换为可插入的形式
        Object[] temp = new Object[attrNum];
        for (int i = 0; i < attrId.length; i++) {
            temp[attrId[i]] = tuple.tuple[i];
        }
        tuple.setTuple(tuple.tuple.length, tupleid, classId, temp);

        // 1.3 元组插入操作
        memConnect.InsertTuple(tuple);
        MemConnect.getObjectTableList().add(new ObjectTableItem(classId, tupleid));

        // 2.找到所有的代理类，进行递归插入
        // 2.1 找到源类所有的代理类
        ArrayList<Integer> DeputyIdList = memConnect.getDeputyIdList(classId);
        String[][] DeputyTypeList = memConnect.getDeputyTypeList(classId);

        // 2.2 将元组转换为代理类应有的形式并递归插入
        if (!DeputyIdList.isEmpty()) {
            for (int i = 0; i < DeputyIdList.size(); i++) {
                int deputyClassId = DeputyIdList.get(i);
                String[] deputyRules = DeputyTypeList[i];

                for (String deputyRule: deputyRules){
                    if (deputyRule.equals("0")) { //Select Deputy
                        HashMap<String, String> attrNameHashMap = getAttrNameHashMap(classId, deputyClassId, columns);
                        List<String> deputyColumns = getDeputyColumns(attrNameHashMap, columns);    // 根据源类属性名列表获取代理类属性名列表
                        Tuple deputyTuple = getSelectDeputyTuple(attrNameHashMap, tuple, columns);        // 将插入源类的元组tuple转换为插入代理类的元组deputyTuple

                        // 递归插入
                        int DeputyTupleId = execute(deputyClassId, deputyColumns, deputyTuple);
                        MemConnect.getBiPointerTableList().add(new BiPointerTableItem(classId, tupleid, deputyClassId, DeputyTupleId));
                    }

                    else if(deputyRule.equals("1")){ //Join Deputy
                        String deputyDetailRule = memConnect.getDetailDeputyRule(deputyClassId);    // 获取join的详细规则
                        // 这里需要修改,应该先join， 然后再插入join的结果
                        List<String> deputyColumns = memConnect.getColumns(deputyClassId);    // join的结果的属性名列表
                        Integer anotherClassId = memConnect.getAnotherOriginID(deputyClassId, classId);    // join的结果的另一个源类id
                        List<Tuple> deputyTupleList = getDeputyJoinTupleList(classId,tuple, anotherClassId,select,deputyDetailRule);    // 获取join的结果
                        for (Tuple deputyTuple : deputyTupleList) {
                            int DeputyTupleId = execute(deputyClassId, deputyColumns, deputyTuple);
                            MemConnect.getBiPointerTableList().add(new BiPointerTableItem(classId, tupleid, deputyClassId, DeputyTupleId));
                            //MemConnect.getBiPointerTableList().add(new BiPointerTableItem(anotherClassId, tupleid, deputyClassId, DeputyTupleId));
                        }
                    }

                }
            }

        }
        return tupleid;
    }

    /**
     * Join the given tuple with all tuples of another class
     * @param tuple The given tuple
     * @param anotherClassId The id of the other class
     * @return The list of joined tuples
     * @throws TMDBException If no class is found with the given id, throw an exception
     */
    public List<Tuple> getDeputyJoinTupleList(int thisClassID,Tuple tuple, int anotherClassId, SelectImpl select,String DeputyDetailRule) throws TMDBException {
        List<Tuple> deputyInsertTupleList = new ArrayList<>(); //Result

        //获取另外一个类的所有Tuple->SelectResult
        TupleList anothertuple = new TupleList();
        List<ObjectTableItem> objs= MemConnect.getObjectTableList();
        for (ObjectTableItem obj : objs) {
            if (obj.classid == anotherClassId) {
                anothertuple.addTuple(memConnect.GetTuple(obj.tupleid));
            }
        }
        List<ClassTableItem> classTableItems = memConnect.getClassTableList(); // Assuming this method returns a list of all class table entries
        SelectResult  right = getSelectResultInformation(anotherClassId, classTableItems, anothertuple);

        //构建本类的SelectResult
        TupleList thisTupleList = new TupleList();
        thisTupleList.addTuple(tuple);
        SelectResult  left =  getSelectResultInformation(thisClassID, classTableItems, thisTupleList);

        SelectImpl selectImpl = new SelectImpl();
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(DeputyDetailRule.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);
            SelectBody selectBody = ((net.sf.jsqlparser.statement.select.Select)stmt).getSelectBody();
            PlainSelect plainSelect = (PlainSelect) selectBody;
            ArrayList<ClassTableItem> leftClassTableItemList = memConnect.copyClassTableList(left.getClassName()[0]);
            ArrayList<ClassTableItem> rightClassTableItemList = memConnect.copyClassTableList(right.getClassName()[0]);
            leftClassTableItemList.addAll(rightClassTableItemList);
            if(!(plainSelect.getJoins() == null)){
                for (Join join:plainSelect.getJoins()) {
                    TupleList tupleList = selectImpl.join(left,right,join);
                    deputyInsertTupleList = tupleList.tuplelist;
                    //接下来还需要选择和投影捏
                    left=select.getSelectResult(leftClassTableItemList, tupleList);
                }
            }
            if (plainSelect.getWhere() != null){
                Where where = new Where();
                left = where.where(plainSelect, left);
            }
            left = select.projection(plainSelect, left);
        }
        catch (JSQLParserException e) {
            System.out.println("syntax error");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        deputyInsertTupleList = left.getTpl().tuplelist;
        return deputyInsertTupleList;
    }

    private SelectResult getSelectResultInformation(int thisClassID, List<ClassTableItem> classTableItems, TupleList thisTupleList) {
        List<String> thisClassName = new ArrayList<>();     // 字段所属的类名
        List<String> thisAttrname = new ArrayList<>();      // 字段名
        List<String> thisAlias = new ArrayList<>();         // 字段的别名，在进行select时会用到
        List<Integer> thisAttrid = new ArrayList<>();       // 显示时使用
        List<String> thisType = new ArrayList<>();          // 字段数据类型(char, int)
        for (ClassTableItem item : classTableItems) {
            if (item.classid == thisClassID) {
                thisClassName.add(item.classname);
                thisAttrname.add(item.attrname);
                thisAlias.add(item.alias);
                thisAttrid.add(item.attrid);
                thisType.add(item.attrtype);
            }
        }
        SelectResult thisResult = new SelectResult(thisTupleList,
                thisClassName.toArray(new String[0]),
                thisAttrname.toArray(new String[0]),
                thisAlias.toArray(new String[0]),
                thisAttrid.stream().mapToInt(i->i).toArray(),
                thisType.toArray(new String[0]));
        return thisResult;
    }

    /**
     * 获取源类属性列表->代理类属性列表的哈希映射列表（注：可能有的源类属性不在代理类中）
     * @param originClassId 源类的class id
     * @param deputyClassId 代理类的class id
     * @param originColumns 源类属性名列表
     * @return 源类属性列表->代理类属性列表的哈希映射列表
     */
    private HashMap<String, String> getAttrNameHashMap(int originClassId, int deputyClassId, List<String> originColumns) {
        HashMap<String, String> attrNameHashMap = new HashMap<>();
        for (SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList()) {
            if (switchingTableItem.oriId != originClassId || switchingTableItem.deputyId != deputyClassId) {
                continue;
            }

            for (String originColumn : originColumns) {
                if (switchingTableItem.oriAttr.equals(originColumn)) {
                    attrNameHashMap.put(originColumn, switchingTableItem.oriAttr);
                }
            }
        }
        return attrNameHashMap;
    }

    /**
     * 给定源类属性名列表，获取其代理类对应属性名列表（注：源类中有的属性可能不在代理类中）
     * @param attrNameHashMap 源类属性名->代理类属性名的哈希表
     * @param originColumns 源类属性名列表
     * @return 代理类属性名列表（注：源类中有的属性可能不在代理类中）
     */
    private List<String> getDeputyColumns(HashMap<String, String> attrNameHashMap, List<String> originColumns) {
        List<String> deputyColumns = new ArrayList<>();
        for (String originColumn : originColumns) {
            if (attrNameHashMap.containsKey(originColumn)){
                deputyColumns.add(attrNameHashMap.get(originColumn));
            }
        }
        return deputyColumns;
    }

    /**
     * 将插入源类的元组转换为插入代理类的元组
     * @param attrNameHashMap 源类属性名->代理类属性名的哈希表
     * @param originTuple 插入源类中的tuple
     * @param originColumns 源类属性名列表
     * @return 能够插入代理类的tuple
     */
    private Tuple getSelectDeputyTuple(HashMap<String, String> attrNameHashMap, Tuple originTuple, List<String> originColumns) {
        Tuple deputyTuple = new Tuple();
        Object[] temp = new Object[attrNameHashMap.size()];
        int i = 0;
        for(String originColumn : originColumns){
            temp[i] = originTuple.tuple[originColumns.indexOf(originColumn)];
            i++;
        }
        deputyTuple.tuple = temp;
        return deputyTuple;
    }
}
