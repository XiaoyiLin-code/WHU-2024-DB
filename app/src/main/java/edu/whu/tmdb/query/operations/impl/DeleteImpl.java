package edu.whu.tmdb.query.operations.impl;


import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Delete;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;



public class DeleteImpl implements Delete {
    private MemConnect memConnect;

    public DeleteImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public void delete(Statement statement) throws JSQLParserException, TMDBException, IOException {
        execute((net.sf.jsqlparser.statement.delete.Delete) statement);
    }

    public void execute(net.sf.jsqlparser.statement.delete.Delete deleteStmt) throws JSQLParserException, TMDBException, IOException {
        // 1.获取符合where条件的所有元组
        Table table = deleteStmt.getTable();        // 获取需要删除的表名
        Expression where = deleteStmt.getWhere();   // 获取delete中的where表达式
        String sql = "select * from " + table;
        ;
        if (where != null) {
            sql += " where " + String.valueOf(where) + ";";
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select = new SelectImpl();
        SelectResult selectResult = select.select(parse);

        // 2.执行delete
        int classId = memConnect.getClassId(table.getName());
        for (Tuple tuple : selectResult.getTpl().tuplelist) {
            delete(classId, tuple.tupleId);
        }
    }


    /**
     * @param classId 表/类id
     * @param tupleid 要删除的元组id
     */
    public void delete(int classId, Integer tupleid) throws TMDBException, IOException {
        //1. Delete tuples from objtable
        List<ObjectTableItem> objlist = MemConnect.getObjectTableItemByTuple(tupleid);
        List<Integer> objidlist = new ArrayList<>();

        MemConnect.getObjectTable().objectTableList.removeAll(objlist);
        //2. Delete tuples
        memConnect.DeleteTuple(tupleid);

        //3. Recursively delete tuples from deputy classes and biPointerTable
        ArrayList<Integer> DeputyIdList = memConnect.getDeputyIdList(classId);
        if (!DeputyIdList.isEmpty()) {
            for (int deputyClassId : DeputyIdList) {
                //choose all deputy tuple id from biPointerTable
                List<Integer> deputyTupleIdList = new ArrayList<>(); // 需要删除的元组，递归删除
                List<BiPointerTableItem> biPointerTableList = MemConnect.getBiPointerTable().biPointerTableList;

                for (int i = 0; i< biPointerTableList.size();i++){
                    BiPointerTableItem biPointerTableItem = biPointerTableList.get(i);
                    if (biPointerTableItem.classid == classId
                            && biPointerTableItem.objectid == tupleid
                            && biPointerTableItem.deputyid == deputyClassId) {
                        deputyTupleIdList.add(biPointerTableItem.deputyobjectid);//add deputy tuple id
                        biPointerTableList.remove(i);//delete from biPointerTable
                        i--;
                    }
                }

//                for (BiPointerTableItem biPointerTableItem : biPointerTableList) {
//                    if (biPointerTableItem.classid == classId
//                            && biPointerTableItem.objectid == tupleid
//                            && biPointerTableItem.deputyid == deputyClassId) {
//                        deputyTupleIdList.add(biPointerTableItem.deputyobjectid);//add deputy tuple id
//                        MemConnect.getBiPointerTable().biPointerTableList.remove(biPointerTableItem);//delete from biPointerTable
//                    }
//                }

                if(!deputyTupleIdList.isEmpty()){
                    for (Integer deputyTupleId : deputyTupleIdList) {
                        delete(deputyClassId, deputyTupleId);
                    }
                }
            }
        }
    }
}


