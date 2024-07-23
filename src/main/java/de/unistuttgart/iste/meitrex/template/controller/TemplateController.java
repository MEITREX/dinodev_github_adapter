package de.unistuttgart.iste.meitrex.template.controller;

import de.unistuttgart.iste.meitrex.generated.dto.Template;
import de.unistuttgart.iste.meitrex.template.service.TemplateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

@Slf4j
@Controller
@RequiredArgsConstructor
public class TemplateController {

    private final TemplateService templateService;

    @QueryMapping
    public List<Template> templates() {
        log.info("Request for all templates");

        return templateService.getAllTemplates();
    }
}
