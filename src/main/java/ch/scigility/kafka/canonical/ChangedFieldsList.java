package ch.scigility.kafka.canonical;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "fieldId",
    "fieldType",
    "fieldValue",
    "fieldChanged"
})
public class ChangedFieldsList implements Serializable
{

    @JsonProperty("fieldId")
    private String fieldId;
    @JsonProperty("fieldType")
    private String fieldType;
    @JsonProperty("fieldValue")
    private String fieldValue;
    @JsonProperty("fieldChanged")
    private String fieldChanged;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    private final static long serialVersionUID = 7233267677088639321L;

    @JsonProperty("fieldId")
    public String getFieldId() {
        return fieldId;
    }

    @JsonProperty("fieldId")
    public void setFieldId(String fieldId) {
        this.fieldId = fieldId;
    }

    public ChangedFieldsList withFieldId(String fieldId) {
        this.fieldId = fieldId;
        return this;
    }

    @JsonProperty("fieldType")
    public String getFieldType() {
        return fieldType;
    }

    @JsonProperty("fieldType")
    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public ChangedFieldsList withFieldType(String fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    @JsonProperty("fieldValue")
    public String getFieldValue() {
        return fieldValue;
    }

    @JsonProperty("fieldValue")
    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public ChangedFieldsList withFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
        return this;
    }

    @JsonProperty("fieldChanged")
    public String getFieldChanged() {
        return fieldChanged;
    }

    @JsonProperty("fieldChanged")
    public void setFieldChanged(String fieldChanged) {
        this.fieldChanged = fieldChanged;
    }

    public ChangedFieldsList withFieldChanged(String fieldChanged) {
        this.fieldChanged = fieldChanged;
        return this;
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

    public ChangedFieldsList withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
