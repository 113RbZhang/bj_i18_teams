package com.rb.black_list.utils.siliconflow;

import static com.rb.black_list.utils.siliconflow.SiliconFlowApi.generateBadReview;

/**
 * @Package com.stream.utils.CommonGenerateTempLate
 * @Author runbo.zhang
 * @Date 2025/3/16 19:43
 * @description: TempLate
 */
public class CommonGenerateTempLate {

    private static final String COMMENT_TEMPLATE = "生成一个电商%s,商品名称为%s,20字数以内,%s不需要思考过程 ";

    private static final String COMMENT = "差评";

    private static final String API_TOKEN = "sk-btoehipugrdkgryyacbkmvccynkdcsdlzayiifzbnaboacwh";

    public static String GenerateComment(String comment,String productName){
        if (COMMENT.equals(comment)){
            return generateBadReview(
                    String.format(COMMENT_TEMPLATE,COMMENT, productName, "攻击性拉满,使用脏话"),
                    API_TOKEN
            );
        }
        return generateBadReview(
                String.format(COMMENT_TEMPLATE,COMMENT, productName,""),
                API_TOKEN
        );
    }

//    public static void main(String[] args) {
//        String s = GenerateComment("差评", "红米k60");
//        System.out.println(s);
//    }

}
