package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.service.QueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controller for querying data.
 */
@Controller
@RequestMapping("/query")
@Slf4j
public class QueryController {

    private final QueryService queryService;

    @Autowired
    public QueryController(QueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * Displays the query page.
     *
     * @param model The model
     * @return The query page
     */
    @GetMapping
    public String queryPage(Model model) {
        model.addAttribute("filters", Map.of());
        model.addAttribute("results", List.of());
        return "query/index";
    }
    
    /**
     * Displays the advanced query page.
     *
     * @param model The model
     * @return The advanced query page
     */
    @GetMapping("/advanced")
    public String advancedQueryPage(Model model) {
        model.addAttribute("filters", Map.of());
        model.addAttribute("results", List.of());
        return "query/advanced";
    }

    /**
     * Handles query execution.
     *
     * @param filters The filters to apply to the query
     * @param limit The maximum number of results to return
     * @param model The model
     * @return The query page with results
     */
    @PostMapping
    public String executeQuery(@RequestParam Map<String, Object> filters,
                              @RequestParam(defaultValue = "100") int limit,
                              Model model) {
        List<Map<String, Object>> results = queryService.executeQuery(filters, limit);
        model.addAttribute("filters", filters);
        model.addAttribute("results", results);
        return "query/index";
    }

    /**
     * REST API for executing a query.
     *
     * @param filters The filters to apply to the query
     * @param limit The maximum number of results to return
     * @return The query results
     */
    @PostMapping("/api")
    @ResponseBody
    public ResponseEntity<List<Map<String, Object>>> executeQueryApi(@RequestBody Map<String, Object> filters,
                                                                   @RequestParam(defaultValue = "100") int limit) {
        List<Map<String, Object>> results = queryService.executeQuery(filters, limit);
        return new ResponseEntity<>(results, HttpStatus.OK);
    }

    /**
     * REST API for executing a bulk query.
     *
     * @param filtersList The list of filters to apply to the query
     * @param limit The maximum number of results to return per query
     * @return The query results
     */
    @PostMapping("/api/bulk")
    @ResponseBody
    public ResponseEntity<List<List<Map<String, Object>>>> executeBulkQueryApi(@RequestBody List<Map<String, Object>> filtersList,
                                                                             @RequestParam(defaultValue = "100") int limit) {
        List<List<Map<String, Object>>> results = queryService.executeBulkQuery(filtersList, limit);
        return new ResponseEntity<>(results, HttpStatus.OK);
    }
}
