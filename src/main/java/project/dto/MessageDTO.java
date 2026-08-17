package project.dto;

import java.util.UUID;

public record MessageDTO(Long id, UUID uuid, boolean isActive, String about) {
}
