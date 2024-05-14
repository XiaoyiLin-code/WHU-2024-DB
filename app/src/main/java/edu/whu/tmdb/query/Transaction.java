package edu.whu.tmdb.query;


import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import edu.whu.tmdb.query.operations.impl.*;
import edu.whu.tmdb.query.operations.torch.TorchConnect;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.Log.LogManager;
import edu.whu.tmdb.query.operations.Create;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Delete;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.Update;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.show.PrintResult;
import edu.whu.tmdb.show.ShowTable;
import edu.whu.tmdb.storage.level.LevelManager;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTable;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTable;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTable;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTable;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTable;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transaction {
    Context context;

    private static Logger logger = LoggerFactory.getLogger(Transaction.class);
    public MemManager mem;
    public LevelManager levelManager;
    public LogManager log;
    private MemConnect memConnect;

    // 1. 私有静态变量，用于保存MemConnect的单一实例
    private static volatile Transaction instance = null;        // volatile关键字使线程对 instance 的修改对其他线程立刻可见

    // 2. 提供一个全局访问点
    public static Transaction getInstance(Context context){

        // 双重检查锁定模式
        try {
            if (instance == null) { // 第一次检查
                synchronized (Transaction.class) {
                    if (instance == null) { // 第二次检查
                        instance = new Transaction();
                        instance.context=context;
                    }
                }
            }
            return instance;
        }catch (TMDBException e){
            // logger.warn(e.getMessage());
            e.printError();
        }catch (JSQLParserException e){
            logger.warn(e.getMessage());
        }catch (IOException e){
            logger.error(e.getMessage());
        }
        return instance;
    }

    public static Transaction getInstance(){

        // 双重检查锁定模式
        try {
            if (instance == null) { // 第一次检查
                synchronized (Transaction.class) {
                    if (instance == null) { // 第二次检查
                        instance = new Transaction();
                    }
                }
            }
            return instance;
        }catch (TMDBException e){
            // logger.warn(e.getMessage());
            e.printError();
        }catch (JSQLParserException e){
            logger.warn(e.getMessage());
        }catch (IOException e){
            logger.error(e.getMessage());
        }
        return instance;
    }
    private Transaction() throws IOException, JSQLParserException, TMDBException {
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }
        this.mem = MemManager.getInstance();
        this.levelManager = MemManager.levelManager;
        this.memConnect = MemConnect.getInstance(mem);
    }


    public void clear() {
//        File classtab=new File("/data/data/edu.whu.tmdb/transaction/classtable");
//        classtab.delete();
        File objtab=new File("/data/data/edu.whu.tmdb/transaction/objecttable");
        objtab.delete();
    }

    public void SaveAll( ) { memConnect.SaveAll(); }

    public void reload() { memConnect.reload(); }

    public void Test(){
        TupleList tpl = new TupleList();
        Tuple t1 = new Tuple();
        t1.tupleSize = 5;
        t1.tuple = new Object[t1.tupleSize];
        t1.tuple[0] = "a";
        t1.tuple[1] = 1;
        t1.tuple[2] = "b";
        t1.tuple[3] = 3;
        t1.tuple[4] = "e";
        Tuple t2 = new Tuple();
        t2.tupleSize = 5;
        t2.tuple = new Object[t2.tupleSize];
        t2.tuple[0] = "d";
        t2.tuple[1] = 2;
        t2.tuple[2] = "e";
        t2.tuple[3] = 2;
        t2.tuple[4] = "v";

        tpl.addTuple(t1);
        tpl.addTuple(t2);
        String[] attrname = {"attr2","attr1","attr3","attr5","attr4"};
        int[] attrid = {1,0,2,4,3};
        String[]attrtype = {"int","char","char","char","int"};

    }

    public SelectResult query(String s) throws JSQLParserException {
        // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
        Statement stmt= CCJSqlParserUtil.parse(byteArrayInputStream);

        return this.query("", -1, stmt);
    }

    public SelectResult query(Statement s) {
        return this.query("", -1, s);
    }

    public SelectResult query(String k, int op, Statement stmt) {
        SelectResult selectResult = null;
        try {
            // 获取生成语法树的类型，用于进一步判断
            String sqlType = stmt.getClass().getSimpleName();

            switch (sqlType) {
                case "CreateTable":
//                    log.WrteLog(s);
                    Create create = new CreateImpl();
                    if(create.create(stmt)) new AlertDialog.Builder(context).setTitle("提示").setMessage("创建成功").setPositiveButton("确定",null).show();
                    else new AlertDialog.Builder(context).setTitle("提示").setMessage("创建失败").setPositiveButton("确定",null).show();
                    break;
                case "CreateDeputyClass":
//                    log.WriteLog(id,k,op,s);
                    CreateDeputyClass createDeputyClass = new CreateDeputyClassImpl();
                    if(createDeputyClass.createDeputyClass(stmt)) {
                        new AlertDialog.Builder(context).setTitle("提示").setMessage("创建成功").setPositiveButton("确定",null).show();
                    }
                    break;
                case "CreateTJoinDeputyClass":
                    // log.WriteLog(id,k,op,s);
                    CreateTJoinDeputyClassImpl createTJoinDeputyClass = new CreateTJoinDeputyClassImpl();
                    if(createTJoinDeputyClass.createTJoinDeputyClass(stmt)){
                        new AlertDialog.Builder(context).setTitle("提示").setMessage("TJoin代理类创建成功").setPositiveButton("确定",null).show();

                    }
                    break;
                case "Drop":
//                    log.WriteLog(id,k,op,s);
                    Drop drop = new DropImpl();
                    drop.drop(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("删除成功").setPositiveButton("确定",null).show();
                    break;
                case "Insert":
//                    log.WriteLog(id,k,op,s);
                    Insert insert = new InsertImpl();
                    insert.insert(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("插入成功").setPositiveButton("确定",null).show();
                    break;
                case "Delete":
 //                   log.WriteLog(id,k,op,s);
                    Delete delete = new DeleteImpl();
                    delete.delete(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("删除成功").setPositiveButton("确定",null).show();
                    break;
                case "Select":
                    Select select = new SelectImpl();
                    selectResult = select.select(stmt);
                    this.PrintSelectResult(selectResult);
                    break;
                case "Update":
 //                   log.WriteLog(id,k,op,s);
                    Update update = new UpdateImpl();
                    update.update(stmt);
                    new AlertDialog.Builder(context).setTitle("提示").setMessage("更新成功").setPositiveButton("确定",null).show();
                    break;
                default:
                    break;
            }
        } catch (JSQLParserException e) {
            logger.warn(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        } catch (TMDBException e) {
            e.printError();
        }

        return selectResult;
    }

    public void testMapMatching() {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
//        torchConnect.insert("data/res/raw/porto_raw_trajectory.txt");
//        this.SaveAll();
        torchConnect.mapMatching();
    }

    public void testEngine() throws IOException {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.initEngine();
    }

    public void insertIntoTrajTable() {
        TorchConnect torchConnect = new TorchConnect(memConnect,"Torch_Porto_test");
        torchConnect.insert("data/res/raw/porto_raw_trajectory.txt");
    }
    private void PrintSelectResult(SelectResult selectResult) {
        Intent intent = new Intent(context, PrintResult.class);
        Bundle bundle = new Bundle();
        bundle.putSerializable("tupleList", selectResult.getTpl());
        bundle.putStringArray("attrname", selectResult.getAttrname());
        bundle.putIntArray("attrid", selectResult.getAttrid());
        bundle.putStringArray("type", selectResult.getType());
        intent.putExtras(bundle);
        context.startActivity(intent);
    }
    public void PrintTab(){
        PrintTab(MemConnect.getObjectTable(),MemConnect.getSwitchingTable(),MemConnect.getDeputyTable(),MemConnect.getBiPointerTable(),memConnect.getClassTable());
    }

    private void PrintTab(ObjectTable topt, SwitchingTable switchingT, DeputyTable deputyt, BiPointerTable biPointerT, ClassTable classTable) {
        Intent intent = new Intent(context, ShowTable.class);
        Bundle bundle0 = new Bundle();
        bundle0.putSerializable("ObjectTable",topt);
        bundle0.putSerializable("SwitchingTable",switchingT);
        bundle0.putSerializable("DeputyTable",deputyt);
        bundle0.putSerializable("BiPointerTable",biPointerT);
        bundle0.putSerializable("ClassTable",classTable);
        intent.putExtras(bundle0);
        context.startActivity(intent);
    }
}

