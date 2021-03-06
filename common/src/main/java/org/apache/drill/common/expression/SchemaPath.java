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
package org.apache.drill.common.expression;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.Iterators;

public class SchemaPath extends LogicalExpressionBase {

  private final NameSegment rootSegment;


  public static SchemaPath getSimplePath(String name){
    return getCompoundPath(name);
  }

  public static SchemaPath getCompoundPath(String... strings){
    List<String> paths = Arrays.asList(strings);
    Collections.reverse(paths);
    NameSegment s = null;
    for(String p : paths){
      s = new NameSegment(p, s);
    }
    return new SchemaPath(s);
  }



  /**
   *
   * @param simpleName
   */
  @Deprecated
  public SchemaPath(String simpleName, ExpressionPosition pos){
    super(pos);
    this.rootSegment = new NameSegment(simpleName);
    if(simpleName.contains(".")) throw new IllegalStateException("This is deprecated and only supports simpe paths.");
  }

  public SchemaPath(SchemaPath path){
    super(path.getPosition());
    this.rootSegment = path.rootSegment;
  }

  public SchemaPath(NameSegment rootSegment){
    super(ExpressionPosition.UNKNOWN);
    this.rootSegment = rootSegment;
  }

  public SchemaPath(NameSegment rootSegment, ExpressionPosition pos){
    super(pos);
    this.rootSegment = rootSegment;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitSchemaPath(this, value);
  }

  public SchemaPath getChild(String childPath){
    NameSegment newRoot = rootSegment.cloneWithNewChild(new NameSegment(childPath));
    return new SchemaPath(newRoot);
  }

  public SchemaPath getChild(int index){
    NameSegment newRoot = rootSegment.cloneWithNewChild(new ArraySegment(index));
    return new SchemaPath(newRoot);
  }

  public NameSegment getRootSegment() {
    return rootSegment;
  }

  @Override
  public MajorType getMajorType() {
    return Types.LATE_BIND_TYPE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((rootSegment == null) ? 0 : rootSegment.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if ( !(obj instanceof SchemaPath))
      return false;
    SchemaPath other = (SchemaPath) obj;
    if (rootSegment == null) {
      if (other.rootSegment != null)
        return false;
    } else if (!rootSegment.equals(other.rootSegment))
      return false;
    return true;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }


  @Override
  public String toString() {
    String expr = ExpressionStringBuilder.toString(this);
    return "SchemaPath ["+ expr + "]";
  }

  public String toExpr(){
    return ExpressionStringBuilder.toString(this);
  }

  public String getAsUnescapedPath(){
    StringBuilder sb = new StringBuilder();
    PathSegment seg = getRootSegment();
    if(seg.isArray()) throw new IllegalStateException("Drill doesn't currently support top level arrays");
    sb.append(seg.getNameSegment().getPath());

    while( (seg = seg.getChild()) != null){
      if(seg.isNamed()){
        sb.append('.');
        sb.append(seg.getNameSegment().getPath());
      }else{
        sb.append('[');
        sb.append(seg.getArraySegment().getIndex());
        sb.append(']');
      }
    }
    return sb.toString();
  }


  public static class De extends StdDeserializer<SchemaPath> {
    DrillConfig config;

    public De(DrillConfig config) {
      super(LogicalExpression.class);
      this.config = config;
    }

    @Override
    public SchemaPath deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String expr = jp.getText();

      if (expr == null || expr.isEmpty())
        return null;
      try {
        // logger.debug("Parsing expression string '{}'", expr);
        ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExprParser parser = new ExprParser(tokens);

        //TODO: move functionregistry and error collector to injectables.
        //ctxt.findInjectableValue(valueId, forProperty, beanInstance)
        parse_return ret = parser.parse();

        // ret.e.resolveAndValidate(expr, errorCollector);
        if(ret.e instanceof SchemaPath){
          return (SchemaPath) ret.e;
        }else{
          throw new IllegalStateException("Schema path is not a valid format.");
        }
      } catch (RecognitionException e) {
        throw new RuntimeException(e);
      }
    }

  }

}