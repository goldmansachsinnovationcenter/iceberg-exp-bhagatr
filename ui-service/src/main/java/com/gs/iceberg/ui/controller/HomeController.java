package com.gs.iceberg.ui.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Controller for the home page.
 */
@Controller
public class HomeController {

    /**
     * Displays the home page.
     *
     * @param model The model
     * @return The home page
     */
    @GetMapping("/")
    public String home(Model model) {
        return "home";
    }
}
