// ORM class for table 'login'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Apr 16 11:15:20 IST 2014
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class login extends SqoopRecord  implements DBWritable, Writable {
public String mask_username(final String text){
//public String mask(final String text){
mask_strings.put("anuraag", "banfrog");//	mask_strings.put("root", "mama");
//	mask_strings.put("anuraag", "banfrog");
	Map<String, String> mask_strings = new HashMap<String, String>();

	for (Map.Entry<String, String> entry : mask_strings.entrySet()) {
		String key = entry.getKey();
		String value = entry.getValue();
		System.out.println(key+" : "+value);
		if(text.toLowerCase().contains(key.toLowerCase() ) ){
			int index = text.indexOf(key);
			return ( text.substring(0,index)+value+text.substring(index+key.length(),text.length() ) );
		}
	}
	return text;
}
  private static final String KEY = "some";
  public String encrypt1(final String text) {
  String newString = text.substring(2,text.length())+text.substring(0,2);
  System.out.println(newString + " " + Integer.toString(text.length() ) );
  return newString;
  }

  private static final Integer KEY1 = 10;
  public Integer encrypt2(final Integer t) {
    Integer apple = t^KEY1;
    return apple;
  }

  public String decrypt1(final String text) {
	System.out.println("this is:"+text+"happy?");
        String newString = text.substring(text.length()-2,text.length())+text.substring(0,text.length()-2);
        return newString;
  }

  public Integer decrypt2(final Integer hash) {

      Integer decipher = KEY1 ^ hash;
      return decipher;
  }

  private byte[] xor(final byte[] input) {

    final byte[] output = new byte[input.length];
    final byte[] secret = this.KEY.getBytes();
    int spos = 0;
    for (int pos = 0; pos < input.length; ++pos) {
      output[pos] = (byte) (input[pos] ^ secret[spos]);
      spos += 1;
      if (spos >= secret.length) {
        spos = 0;
      }
    }
	System.out.println("t"+new String(output));
    return output;
  }

  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public login with_id(Integer id) {
    this.id = id;
    return this;
  }
  private String username;
  public String get_username() {
    return username;
  }
  public void set_username(String username) {
    this.username = username;
  }
  public login with_username(String username) {
    this.username = username;
    return this;
  }
  private String password;
  public String get_password() {
    return password;
  }
  public void set_password(String password) {
    this.password = password;
  }
  public login with_password(String password) {
    this.password = password;
    return this;
  }
  private String status;
  public String get_status() {
    return status;
  }
  public void set_status(String status) {
    this.status = status;
  }
  public login with_status(String status) {
    this.status = status;
    return this;
  }
  private String email;
  public String get_email() {
    return email;
  }
  public void set_email(String email) {
    this.email = email;
  }
  public login with_email(String email) {
    this.email = email;
    return this;
  }
  private Integer org_id;
  public Integer get_org_id() {
    return org_id;
  }
  public void set_org_id(Integer org_id) {
    this.org_id = org_id;
  }
  public login with_org_id(Integer org_id) {
    this.org_id = org_id;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof login)) {
      return false;
    }
    login that = (login) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.username == null ? that.username == null : this.username.equals(that.username));
    equal = equal && (this.password == null ? that.password == null : this.password.equals(that.password));
    equal = equal && (this.status == null ? that.status == null : this.status.equals(that.status));
    equal = equal && (this.email == null ? that.email == null : this.email.equals(that.email));
    equal = equal && (this.org_id == null ? that.org_id == null : this.org_id.equals(that.org_id));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = encrypt2(JdbcWritableBridge.readInteger(1, __dbResults));    
    this.username = JdbcWritableBridge.readString(2, __dbResults);
    this.password = JdbcWritableBridge.readString(3, __dbResults);
    this.status = JdbcWritableBridge.readString(4, __dbResults);
    this.email = JdbcWritableBridge.readString(5, __dbResults);
    this.org_id = JdbcWritableBridge.readInteger(6, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
JdbcWritableBridge.writeInteger(decrypt2(id), 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(username, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(password, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(status, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(email, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(org_id, 6 + __off, 4, __dbStmt);
    return 6;
  }
  public void readFields(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.username = null;
    } else {
    this.username = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.password = null;
    } else {
    this.password = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.status = null;
    } else {
    this.status = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.email = null;
    } else {
    this.email = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.org_id = null;
    } else {
    this.org_id = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.username) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, username);
    }
    if (null == this.password) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, password);
    }
    if (null == this.status) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, status);
    }
    if (null == this.email) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, email);
    }
    if (null == this.org_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.org_id);
    }
  }
  private final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(username==null?"null":username, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(password==null?"null":password, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(status==null?"null":status, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(email==null?"null":email, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(org_id==null?"null":"" + org_id, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  private final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str;
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.username = null; } else {
      this.username = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.password = null; } else {
      this.password = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.status = null; } else {
      this.status = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.email = null; } else {
      this.email = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.org_id = null; } else {
      this.org_id = Integer.valueOf(__cur_str);
    }

  }

  public Object clone() throws CloneNotSupportedException {
    login o = (login) super.clone();
    return o;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("username", this.username);
    __sqoop$field_map.put("password", this.password);
    __sqoop$field_map.put("status", this.status);
    __sqoop$field_map.put("email", this.email);
    __sqoop$field_map.put("org_id", this.org_id);
    return __sqoop$field_map;
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
    }
    else    if ("username".equals(__fieldName)) {
      this.username = (String) __fieldVal;
    }
    else    if ("password".equals(__fieldName)) {
      this.password = (String) __fieldVal;
    }
    else    if ("status".equals(__fieldName)) {
      this.status = (String) __fieldVal;
    }
    else    if ("email".equals(__fieldName)) {
      this.email = (String) __fieldVal;
    }
    else    if ("org_id".equals(__fieldName)) {
      this.org_id = (Integer) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
}
