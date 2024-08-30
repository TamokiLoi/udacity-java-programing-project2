package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
    private final Clock clock;
    private final Duration timeout;
    private final int popularWordCount;
    private final ForkJoinPool pool;
    private final List<Pattern> ignoredUrls;
    private final int maxDepth;
    private final PageParserFactory parserFactory;

    @Inject
    ParallelWebCrawler(
            Clock clock,
            @Timeout Duration timeout,
            @PopularWordCount int popularWordCount,
            @TargetParallelism int threadCount,
            @IgnoredUrls List<Pattern> ignoredUrls,
            @MaxDepth int maxDepth,
            PageParserFactory parserFactory) {
        this.clock = clock;
        this.timeout = timeout;
        this.popularWordCount = popularWordCount;
        this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
        this.ignoredUrls = ignoredUrls;
        this.maxDepth = maxDepth;
        this.parserFactory = parserFactory;
    }

    @Override
    public CrawlResult crawl(List<String> startingUrls) {
        Instant endTime = clock.instant().plus(timeout);
        ConcurrentMap<String, Integer> wordCounts = new ConcurrentHashMap<>();
        ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();

        for (String url : startingUrls) {
            pool.submit(new InternalCrawler(url, endTime, maxDepth, wordCounts, visitedUrls));
        }

        // Wait for all tasks to complete
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                System.err.println("Crawler tasks did not complete in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Crawler interrupted");
        }

        CrawlResult.Builder resultBuilder = new CrawlResult.Builder()
                .setUrlsVisited(visitedUrls.size());

        if (!wordCounts.isEmpty()) {
            resultBuilder.setWordCounts(WordCounts.sort(wordCounts, popularWordCount));
        }

        return resultBuilder.build();
    }

    public class InternalCrawler extends RecursiveTask<Boolean> {
        private final String url;
        private final Instant deadline;
        private final int maxDepth;
        private final ConcurrentMap<String, Integer> counts;
        private final ConcurrentSkipListSet<String> visitedUrls;

        public InternalCrawler(String url, Instant deadline, int maxDepth, ConcurrentMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls) {
            this.url = url;
            this.deadline = deadline;
            this.maxDepth = maxDepth;
            this.counts = counts;
            this.visitedUrls = visitedUrls;
        }

        @Override
        protected Boolean compute() {
            // Terminate recursion if depth limit is reached or deadline has passed
            if (maxDepth <= 0 || clock.instant().isAfter(deadline)) {
                return false;
            }

            // Skip URLs that match any pattern in ignoredUrls
            for (Pattern pattern : ignoredUrls) {
                if (pattern.matcher(url).matches()) {
                    return false;
                }
            }

            // Add the URL to visited set and process it if it was not visited
            if (!visitedUrls.add(url)) {
                return false;
            }

            // Parse the page and update word counts
            PageParser.Result pageResult = parserFactory.get(url).parse();
            pageResult.getWordCounts().forEach((word, count) ->
                    counts.merge(word, count, Integer::sum)
            );

            // Create subtasks for each link found on the page
            List<InternalCrawler> subtasks = pageResult.getLinks().stream()
                    .map(link -> new InternalCrawler(link, deadline, maxDepth - 1, counts, visitedUrls))
                    .toList();

            // Invoke all subtasks
            invokeAll(subtasks);

            return true;
        }
    }

    @Override
    public int getMaxParallelism() {
        return Runtime.getRuntime().availableProcessors();
    }

}