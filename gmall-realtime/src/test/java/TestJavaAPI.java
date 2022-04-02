
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/2 16:31
 */


public class TestJavaAPI {

    @Test
    public void testJsonClean(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name","zhangsna");
        System.out.println(jsonObject);
        jsonObject.clear();
        System.out.println(jsonObject);
    }
}
