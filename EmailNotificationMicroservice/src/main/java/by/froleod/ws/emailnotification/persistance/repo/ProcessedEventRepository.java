package by.froleod.ws.emailnotification.persistance.repo;

import by.froleod.ws.emailnotification.persistance.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

    ProcessedEventEntity findByMessageId(String messageId);
}
