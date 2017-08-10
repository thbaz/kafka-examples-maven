package ch.scigility.kafka.canonical;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

import ch.scigility.kafka.canonical.avro.ContractsSchema;
import org.joda.time.DateTime;
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "commandScn",
    "commandCommitScn",
    "commandSequence",
    "commandType",
    "commandTimestamp",
    "objectDBName",
    "objectSchemaName",
    "objectId",
    "changedFieldsList",
    "conditionFieldsList"
})

public class Landing implements Serializable
{

    @JsonProperty("commandScn")
    private String commandScn;
    @JsonProperty("commandCommitScn")
    private String commandCommitScn;
    @JsonProperty("commandSequence")
    private String commandSequence;
    @JsonProperty("commandType")
    private String commandType;
    @JsonProperty("commandTimestamp")
    private String commandTimestamp;
    @JsonProperty("objectDBName")
    private String objectDBName;
    @JsonProperty("objectSchemaName")
    private String objectSchemaName;
    @JsonProperty("objectId")
    private String objectId;
    @JsonProperty("changedFieldsList")
    private List<ChangedFieldsList> changedFieldsList = null;
    @JsonProperty("conditionFieldsList")
    private List<Object> conditionFieldsList = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    private final static long serialVersionUID = 8620905031372624204L;

    @JsonProperty("commandScn")
    public String getCommandScn() {
        return commandScn;
    }

    @JsonProperty("commandScn")
    public void setCommandScn(String commandScn) {
        this.commandScn = commandScn;
    }

    public Landing withCommandScn(String commandScn) {
        this.commandScn = commandScn;
        return this;
    }

    @JsonProperty("commandCommitScn")
    public String getCommandCommitScn() {
        return commandCommitScn;
    }

    @JsonProperty("commandCommitScn")
    public void setCommandCommitScn(String commandCommitScn) {
        this.commandCommitScn = commandCommitScn;
    }

    public Landing withCommandCommitScn(String commandCommitScn) {
        this.commandCommitScn = commandCommitScn;
        return this;
    }

    @JsonProperty("commandSequence")
    public String getCommandSequence() {
        return commandSequence;
    }

    @JsonProperty("commandSequence")
    public void setCommandSequence(String commandSequence) {
        this.commandSequence = commandSequence;
    }

    public Landing withCommandSequence(String commandSequence) {
        this.commandSequence = commandSequence;
        return this;
    }

    @JsonProperty("commandType")
    public String getCommandType() {
        return commandType;
    }

    @JsonProperty("commandType")
    public void setCommandType(String commandType) {
        this.commandType = commandType;
    }

    public Landing withCommandType(String commandType) {
        this.commandType = commandType;
        return this;
    }

    @JsonProperty("commandTimestamp")
    public String getCommandTimestamp() {
        return commandTimestamp;
    }

    @JsonProperty("commandTimestamp")
    public void setCommandTimestamp(String commandTimestamp) {
        this.commandTimestamp = commandTimestamp;
    }

    public Landing withCommandTimestamp(String commandTimestamp) {
        this.commandTimestamp = commandTimestamp;
        return this;
    }

    @JsonProperty("objectDBName")
    public String getObjectDBName() {
        return objectDBName;
    }

    @JsonProperty("objectDBName")
    public void setObjectDBName(String objectDBName) {
        this.objectDBName = objectDBName;
    }

    public Landing withObjectDBName(String objectDBName) {
        this.objectDBName = objectDBName;
        return this;
    }

    @JsonProperty("objectSchemaName")
    public String getObjectSchemaName() {
        return objectSchemaName;
    }

    @JsonProperty("objectSchemaName")
    public void setObjectSchemaName(String objectSchemaName) {
        this.objectSchemaName = objectSchemaName;
    }

    public Landing withObjectSchemaName(String objectSchemaName) {
        this.objectSchemaName = objectSchemaName;
        return this;
    }

    @JsonProperty("objectId")
    public String getObjectId() {
        return objectId;
    }

    @JsonProperty("objectId")
    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public Landing withObjectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    @JsonProperty("changedFieldsList")
    public List<ChangedFieldsList> getChangedFieldsList() {
        return changedFieldsList;
    }

    @JsonProperty("changedFieldsList")
    public void setChangedFieldsList(List<ChangedFieldsList> changedFieldsList) {
        this.changedFieldsList = changedFieldsList;
    }

    public Landing withChangedFieldsList(List<ChangedFieldsList> changedFieldsList) {
        this.changedFieldsList = changedFieldsList;
        return this;
    }

    @JsonProperty("conditionFieldsList")
    public List<Object> getConditionFieldsList() {
        return conditionFieldsList;
    }

    @JsonProperty("conditionFieldsList")
    public void setConditionFieldsList(List<Object> conditionFieldsList) {
        this.conditionFieldsList = conditionFieldsList;
    }

    public Landing withConditionFieldsList(List<Object> conditionFieldsList) {
        this.conditionFieldsList = conditionFieldsList;
        return this;
    }

    public ContractsSchema toAvroCanonical(){
      ContractsSchema contractsAvro = new ContractsSchema();
      SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss+00:000");
      //getAdditionalProperties
      for( ChangedFieldsList change: changedFieldsList ){
        switch(change.getFieldId()){
          case "COCO_ID":
            contractsAvro.put(0,new Long(change.getFieldValue()).longValue());
            break;
          case "COCO_TYPE":
            contractsAvro.put(1,new Long(change.getFieldValue()).longValue());
            break;
          case "COOC_COVERAGE":
            contractsAvro.put(2,new Long(change.getFieldValue()).longValue());
            break;
          case "COCO_ANNUAL_PREMIUM":
            contractsAvro.put(3,new Long(change.getFieldValue()).longValue());
            break;
          case "COCO_START_DATE":
            try {
                Date date = f.parse(change.getFieldValue());
                long milliseconds = date.getTime();
                DateTime oradate = new org.joda.time.DateTime(milliseconds);
                contractsAvro.put(4,oradate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            break;
          case "COCO_END_DATE":
            try {
              Date date = f.parse(change.getFieldValue());
              long milliseconds = date.getTime();
              DateTime oradate = new org.joda.time.DateTime(milliseconds);
              contractsAvro.put(5,oradate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            break;
          case "COCO_COCU_ID":
            contractsAvro.put(6,new Long(change.getFieldValue()).longValue());
            break;
          case "COCO_COAG_ID":
            contractsAvro.put(7,new Long(change.getFieldValue()).longValue());
            break;
          case "COCO_TOTAL_PAID_PREMIUMS":
            contractsAvro.put(8,new Long(change.getFieldValue()).longValue());
            break;
          case "COCO_TOTAL_PAID_CLAIMS":
            contractsAvro.put(8,(java.lang.Long)contractsAvro.get(8)-new Long(change.getFieldValue()).longValue());
            break;
        }
      }
      return contractsAvro;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public Landing withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
