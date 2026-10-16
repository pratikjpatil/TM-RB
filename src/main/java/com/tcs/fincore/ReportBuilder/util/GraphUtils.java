package com.tcs.fincore.ReportBuilder.util;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Used by CalculationService (for Cell Formulas) and GlobalParamEngine (for Parameters).
 */
@Slf4j
public class GraphUtils {

    /**
     * Performs a Topological Sort on a dependency graph using Kahn's Algorithm.
     * Detects circular dependencies automatically.
     *
     * @param dependencies Map where Key = Item, Value = Set of items it depends on.
     * @return List of keys sorted by execution order.
     * @throws ValidationException if a cycle is detected.
     */
    public static List<String> sort(Map<String, Set<String>> dependencies) {
        if (dependencies == null || dependencies.isEmpty()) {
            return Collections.emptyList();
        }

        // Initialize In-Degree Count
        Map<String, Integer> inDegree = new HashMap<>();
        dependencies.keySet().forEach(k -> inDegree.put(k, 0));

        //  Build Adjacency List (Reverse dependency: if A depends on B, edge is B -> A)
        // We want to process B first, then A.
        Map<String, List<String>> adjList = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : dependencies.entrySet()) {
            String dependent = entry.getKey();
            Set<String> dependsOn = entry.getValue();

            if (dependsOn != null) {
                for (String dependency : dependsOn) {
                    // Only track dependencies that are actually part of the graph
                    if (dependencies.containsKey(dependency)) {
                        adjList.computeIfAbsent(dependency, k -> new ArrayList<>()).add(dependent);
                        inDegree.merge(dependent, 1, Integer::sum);
                    }
                }
            }
        }

        // Initialize Queue with independent nodes (in-degree 0)
        Deque<String> queue = new ArrayDeque<>();
        inDegree.forEach((node, degree) -> {
            if (degree == 0) queue.add(node);
        });

        // Process Queue
        List<String> sortedOrder = new ArrayList<>(dependencies.size());
        while (!queue.isEmpty()) {
            String u = queue.poll();
            sortedOrder.add(u);

            if (adjList.containsKey(u)) {
                for (String v : adjList.get(u)) {
                    int newDegree = inDegree.get(v) - 1;
                    inDegree.put(v, newDegree);

                    if (newDegree == 0) {
                        queue.add(v);
                    }
                }
            }
        }

        // Cycle Detection
        if (sortedOrder.size() != dependencies.size()) {
            // Find nodes involved in cycle for better error reporting
            List<String> cyclicNodes = inDegree.entrySet().stream()
                    .filter(e -> e.getValue() > 0)
                    .map(Map.Entry::getKey)
                    .toList();

            log.error("Cycle detected involving: {}", cyclicNodes);
            throw new ValidationException("Circular dependency detected in configuration. Check items: " + cyclicNodes);
        }

        return sortedOrder;
    }
}