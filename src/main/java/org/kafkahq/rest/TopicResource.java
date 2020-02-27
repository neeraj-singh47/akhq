package org.kafkahq.rest;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import lombok.extern.slf4j.Slf4j;
import org.kafkahq.repositories.RecordRepository;
import org.kafkahq.service.TopicService;
import org.kafkahq.service.dto.topic.PartitionDTO;
import org.kafkahq.service.dto.topic.RecordDTO;
import org.kafkahq.service.dto.topic.TopicDTO;

import javax.inject.Inject;
import javax.validation.constraints.Null;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
@Controller("${kafkahq.server.base-path:}/api")
public class TopicResource {
    private TopicService topicService;

    @Inject
    public TopicResource(TopicService topicService) {
        this.topicService = topicService;
    }

    @Get("/topicsByType")
    public List<TopicDTO> fetchAllTopicsByType(String clusterId, String view) throws ExecutionException, InterruptedException {
        log.debug("Fetch all topics by type");
        return topicService.getAllTopicsByType(clusterId, view);
    }

    @Get("/topicsByName")
    public List<TopicDTO> fetchAllTopicsByName(String clusterId, String view, String search) throws ExecutionException, InterruptedException {
        log.debug("Fetch all topics by name");
        return topicService.getAllTopicsByName(clusterId, view, search);
    }

    // TODO - Finish endpoint. Strange error while trying to do the route matching.
    @Get("/topic/data")
    public List<RecordDTO> fetchTopicData(String clusterId, String topicId,
                                          @Null Optional<String> after,
                                          @Null Optional<Integer> partition,
                                          @Null Optional<RecordRepository.Options.Sort> sort,
                                          @Null Optional<String> timestamp,
                                          @Null Optional<String> search) throws ExecutionException, InterruptedException {
        log.debug("Fetch data from topic: {}", topicId);
        return topicService.getTopicData(clusterId, topicId, after, partition, sort, timestamp, search);
    }

    @Get("/topic/partitions")
    public List<PartitionDTO> fetchTopicPartitions(String clusterId, String topicId) throws ExecutionException, InterruptedException {
        log.debug("Fetch partitions from topic: {}", topicId);
        return topicService.getTopicPartitions(clusterId, topicId);
    }
}

