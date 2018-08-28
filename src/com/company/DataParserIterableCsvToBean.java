/*************************************************************************
 * Copyright (c) Metabiota Incorporated - All Rights Reserved
 * ------------------------------------------------------------------------
 * This material is proprietary to Metabiota Incorporated. The
 * intellectual and technical concepts contained herein are proprietary
 * to Metabiota Incorporated. Reproduction or distribution of this
 * material, in whole or in part, is strictly forbidden unless prior
 * written permission is obtained from Metabiota Incorporated.
 *************************************************************************/
package com.company;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBeanFilter;
import com.opencsv.bean.IterableCSVToBean;
import com.opencsv.bean.MappingStrategy;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import lombok.Setter;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataParserIterableCsvToBean extends IterableCSVToBean {
    private static final Logger LOG = LogManager.getLogger(DataParserIterableCsvToBean.class);
    @Setter
    private Map<Integer, ParseErrors> parseErrors = new HashMap();

    private AtomicInteger lineNumber = new AtomicInteger(0);

    public DataParserIterableCsvToBean(CSVReader csvReader, MappingStrategy strategy, CsvToBeanFilter filter) {
        super(csvReader, strategy, filter);
    }

    private Boolean canParseValue(String value, Class valueType) {
        if (StringUtils.isNotEmpty(value)) {
            if (valueType.equals(Integer.class)) {
                try {
                    Integer.parseInt(value);
                    return true;
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                    return false;
                }
            } else if(valueType.equals(Long.class)) {
                try {
                    Long.parseLong(value);
                    return true;
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                    return false;
                }
            } else if(valueType.equals(Float.class)) {
                try {
                    Float.parseFloat(value);
                    return true;
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                    return false;
                }
            } else if(valueType.equals(Double.class)) {
                try {
                    Double.parseDouble(value);
                    return true;
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                    return false;
                }
            } else return valueType.equals(String.class);
        } else {
            return true;
        }
    }

    public Set<String> getParseErrors(Integer rowNumber) {
        if (parseErrors.get(rowNumber) != null) {
            Set<String> parseErrorPropNames = parseErrors.get(rowNumber).parseErrorPropNames;
            parseErrors.remove(rowNumber);
            return parseErrorPropNames;
        }
        return new HashSet();
    }

    @Override
    protected Object convertValue(String value, PropertyDescriptor prop) throws InstantiationException, IllegalAccessException {
        Boolean canParse = canParseValue(value, prop.getPropertyType());
        if (!canParse) {
            parseErrors.compute(lineNumber.get(), (k, v) -> v == null ? new ParseErrors(prop.getName()) : v.add(prop.getName()));
        }
        PropertyEditor editor = this.getPropertyEditor(prop);
        Object obj = null;
        if (canParse && editor != null) {
            editor.setAsText(value);
            obj = editor.getValue();
        }
        return obj;
    }

    @Override
    public Object nextLine() throws IllegalAccessException, InstantiationException, IOException, IntrospectionException, InvocationTargetException, CsvRequiredFieldEmptyException {
        lineNumber.incrementAndGet();
        return super.nextLine();
    }

    private static class ParseErrors {
        private Set<String> parseErrorPropNames = new HashSet<>();

        public ParseErrors(String propName) {
            parseErrorPropNames.add(propName);
        }

        public ParseErrors add(String propName) {
            parseErrorPropNames.add(propName);
            return this;
        }
    }
}
