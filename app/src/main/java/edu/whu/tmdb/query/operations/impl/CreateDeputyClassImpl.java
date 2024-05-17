package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.TupleList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private final MemConnect memConnect;

    public CreateDeputyClassImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public boolean createDeputyClass(Statement stmt) throws TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }

    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException, IOException {
        // 1.获取代理类名、代理类型、select元组
        String deputyClassName = stmt.getDeputyClass().toString();  // 代理类名
        if (memConnect.classExist(deputyClassName)) {
            throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
        }
        int deputyType = getDeputyType(stmt);   // 代理类型
        Select selectStmt = stmt.getSelect();
        SelectResult selectResult = getSelectResult(selectStmt);

        // 2.执行代理类创建
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType,stmt.getSelect().toString());
        createDeputyTableItem(deputyClassName,selectResult.getClassName(), deputyType, deputyId);
        createBiPointerTableItem(selectResult, deputyId);
        return true;
    }

    public boolean createDeputyClassStreamLine(SelectResult selectResult, int deputyType, String deputyClassName) throws TMDBException, IOException {
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType,"");
        createDeputyTableItem(deputyClassName,selectResult.getClassName(), deputyType, deputyId);
        createBiPointerTableItem(selectResult, deputyId);
        return true;
    }


    //别名的作用是什么
    /**
     * 创建代理类的实现，包含代理类classTableItem的创建和switchingTableItem的创建
     * @param deputyClassName 代理类名称
     * @param selectResult 代理类包含的元组列表
     * @param deputyRule 代理规则
     * @return 新建代理类ID
     */
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule,String DetailDeputyRule) throws TMDBException {
        // TODO-task3
        // 遍历selectResult

        for (ClassTableItem item : MemConnect.getClassTableList()) {
            if (item.classname.equals(deputyClassName)) {
                throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
            }
        }

        MemConnect.getClassTable().maxid++;
        int classid = MemConnect.getClassTable().maxid;
        int count=selectResult.getAttrid().length;
        int orid=-1;
        for (int i = 0; i < count; i++) {
            // 1.新建classTableItem
            // 使用MemConnect.getClassTableList().add()
            MemConnect.getClassTableList().add(new ClassTableItem(deputyClassName,classid, count,
                    i, selectResult.getAttrname()[i],selectResult.getType()[i]
                    ,"de",""));

            // 2.新建switchingTableItem
            // 使用MemConnect.getSwitchingTableList().add()
            orid=memConnect.getClassId(selectResult.getClassName()[i]);
            MemConnect.getSwitchingTableList().add(new SwitchingTableItem(orid,selectResult.getAttrid()[i],selectResult.getAttrname()[i]
                    ,classid,i,selectResult.getAttrname()[i],String.valueOf(deputyRule)+DetailDeputyRule));
        }
        return classid;
    }




    /**
     * 新建deputyTableItem
     * @param classNames 源类类名列表
     * @param deputyType 代理规则
     * @param deputyId 代理类id
     */
    public void createDeputyTableItem(String deputyClassName,String[] classNames, int deputyType, int deputyId) throws TMDBException {
        HashSet<String> Names = Arrays.stream(classNames).collect(Collectors.toCollection(HashSet::new));
        for(String name:Names){
            int orid=memConnect.getClassId(name);
            MemConnect.getDeputyTableList().add(new DeputyTableItem(deputyClassName,orid,deputyId,new String[]{String.valueOf(deputyType)}));
        }
        // TODO-task3
        // 使用MemConnect.getDeputyTableList().add()
    }

    /**
     * 插入元组，并新建BiPointerTableItem
     * @param selectResult 插入的元组列表
     * @param deputyId 新建代理类id
     */
    private void createBiPointerTableItem(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        // 1. Insert each item in the selectResult
        TupleList tpl=selectResult.getTpl();
        InsertImpl insert=new InsertImpl();
        List<String> columns= Arrays.asList(selectResult.getAttrname());
        for(Tuple tuple:tpl.tuplelist){
            int deputyTupleId= insert.execute(deputyId,columns,new Tuple(tuple.tuple));
            HashSet<Integer> origin = getOriginClass(selectResult);
            for(int origin_index:origin){
                int classId=memConnect.getClassId(selectResult.getClassName()[origin_index]);
                int orinTupleId=tuple.tupleIds[origin_index];
                MemConnect.getBiPointerTableList().add(
                        new BiPointerTableItem(classId,orinTupleId,deputyId,deputyTupleId)
                );
            }
        }

    }
    /**
     * 给定创建代理类语句，返回代理规则
     * @param stmt 创建代理类语句
     * @return 代理规则
     */
    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        switch (stmt.getType().toLowerCase(Locale.ROOT)) {
            case "selectdeputy":    return 0;
            case "joindeputy":      return 1;
            case "uniondeputy":     return 2;
            case "groupbydeputy":   return 3;
        }
        return -1;
    }

    /**
     * 给定查询语句，返回select查询执行结果（创建deputyclass后面的select语句中的selectResult）
     * @param selectStmt select查询语句
     * @return 查询执行结果（包含所有满足条件元组）
     */
    private SelectResult getSelectResult(Select selectStmt) throws TMDBException, IOException {
        SelectImpl selectExecutor = new SelectImpl();
        return selectExecutor.select(selectStmt);
    }

    private HashSet<Integer> getOriginClass(SelectResult selectResult) {
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res = new HashSet<>();
        for (String s : collect1) {
            res.add(collect.indexOf(s));
        }
        return res;
    }
}
