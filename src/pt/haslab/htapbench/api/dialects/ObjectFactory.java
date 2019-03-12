
/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************
/*
 * Copyright 2017 by INESC TEC                                                                                                
 * This work was based on the OLTPBenchmark Project                          
 *
 * Licensed under the Apache License, Version 2.0 (the "License");           
 * you may not use this file except in compliance with the License.          
 * You may obtain a copy of the License at                                   
 *
 * http://www.apache.org/licenses/LICENSE-2.0                              
 *
 * Unless required by applicable law or agreed to in writing, software       
 * distributed under the License is distributed on an "AS IS" BASIS,         
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and       
 * limitations under the License. 
 */
package pt.haslab.htapbench.api.dialects;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the com.oltpbenchmark.api.dialects package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _Dialects_QNAME = new QName("", "dialects");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: com.oltpbenchmark.api.dialects
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link DialectType }
     * 
     */
    public DialectType createDialectType() {
        return new DialectType();
    }

    /**
     * Create an instance of {@link StatementType }
     * 
     */
    public StatementType createStatementType() {
        return new StatementType();
    }

    /**
     * Create an instance of {@link ProcedureType }
     * 
     */
    public ProcedureType createProcedureType() {
        return new ProcedureType();
    }

    /**
     * Create an instance of {@link DialectsType }
     * 
     */
    public DialectsType createDialectsType() {
        return new DialectsType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DialectsType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "", name = "dialects")
    public JAXBElement<DialectsType> createDialects(DialectsType value) {
        return new JAXBElement<DialectsType>(_Dialects_QNAME, DialectsType.class, null, value);
    }

}
