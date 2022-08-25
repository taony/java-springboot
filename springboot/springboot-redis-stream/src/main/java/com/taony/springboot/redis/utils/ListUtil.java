package com.taony.springboot.redis.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListUtil {

    public static List<List<String>> cutApart(int max, List<String> list){
        List<List<String>> resultList = new ArrayList<>();
        Stream.iterate(0, n->n+1).limit(countStep(list.size(), max)).forEach(i->{
            resultList.add(list.stream().skip(i*max).limit(max).collect(Collectors.toList()));
        });
        return resultList;
    }

    private static Integer countStep(int size, int max){
        return (size + max - 1) / max;
    }
}
