package de.unistuttgart.iste.meitrex.template.persistence.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity(name = "Template")
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TemplateEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;

    @Column(nullable = false, length = 255)
    private String name;

}
