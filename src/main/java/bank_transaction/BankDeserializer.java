package bank_transaction;



import serializers.JsonPOJODeserializer;

import java.util.HashMap;
import java.util.Map;

public class BankDeserializer extends JsonPOJODeserializer<StreamApp.BankBalance> {

    public BankDeserializer(){
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonPOJOClass", StreamApp.BankBalance.class);
        configure(serdeProps, false);
    }
}
