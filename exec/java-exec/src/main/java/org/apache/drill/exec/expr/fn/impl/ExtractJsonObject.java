/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;   
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.JavaType; 

 
public class ExtractJsonObject {
 		
	  @FunctionTemplate(name = "getJsonObject", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
	  public static class GetJsonObject implements DrillSimpleFunc{
	    
	    @Param VarBinaryHolder byteMessage;
	    @Param VarCharHolder  field;
	    @Output VarCharHolder out;
	    @Workspace ByteBuf buffer;
	    @Workspace byte[] b;
	    @Workspace String strField;
	    @Workspace String strMessage;
	    @Workspace JsonUtility jsonUtility;
 
	    public void setup(RecordBatch incoming){
 	    	buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte[64000]);
 	    //	jsonUtility=  new JsonUtility();
	     }
	    
	    public void eval(){
	    	int len = byteMessage.end-byteMessage.start;
			b=new byte[len];
			byteMessage.buffer.getBytes(byteMessage.start,b,0,byteMessage.end-byteMessage.start);
 
			strMessage = new String(b);  
			strField = field.toString();
			
			String result=jsonUtility.extractJson(strMessage, strField);
 
			out.buffer=buffer;
			out.start=0;
			out.end=result.length();
			out.buffer.setBytes(0,result.getBytes()); 
 	    } 
	  } 	   
} 

