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

import java.util.Arrays;
import java.util.Optional;

import com.google.common.base.CharMatcher;
import com.opencsv.bean.CsvToBeanFilter;
import org.apache.commons.lang.StringUtils;

public class DataParserCsvToBeanFilter implements CsvToBeanFilter {

    @Override
    public boolean allowLine(String[] strings) {
        Optional<Boolean> reduceResult = Arrays.stream(strings).map(s -> !StringUtils.isEmpty(s) && !CharMatcher.WHITESPACE.matchesAllOf(s)).reduce((left, right) -> left || right);
        return reduceResult.orElse(false);
    }
}
