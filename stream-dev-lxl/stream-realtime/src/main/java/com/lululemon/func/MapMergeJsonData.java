package lululemon.func;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Author: lxl
 * @Date: 2025/10/26 🐕 🎇
 */
public class MapMergeJsonData extends RichMapFunction<JSONObject,JSONObject> {
    @Override
    public JSONObject map(JSONObject data) throws Exception {

        //JSONObject resultJson = new JSONObject();

        if(data.containsKey("after") && data.getJSONObject("after")!=null){
            JSONObject after = data.getJSONObject("after");
            JSONObject source = data.getJSONObject("source");
            String db = source.getString("db");
            String scheam = source.getString("scheam");
            String table = source.getString("table");

            String tablename="";
            tablename=db+"."+scheam+"."+table;

            String op=data.getString("op");
            after.put("table_name",tablename);

            after.put("op",op);

            return after;
        }

        return null;
    }
}
