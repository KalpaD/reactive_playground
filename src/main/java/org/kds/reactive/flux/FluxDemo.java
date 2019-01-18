package org.kds.reactive.flux;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class FluxDemo {

    CountDownLatch countDownLatch;
    StopWatch stopWatch;

    public FluxDemo() {
        countDownLatch = new CountDownLatch(5);
        stopWatch = new StopWatch();
    }

    /**
     * This method demonstrates the filter operation on a Flux by applying a conditional operation on
     * flux stream.
     */
    private void demoFilterOperation() {
        Flux.just(2, 30, 22, 5, 60, 1)
                .filter( x -> x > 10)
                .subscribe( filteredItem -> log.info("Filtered Item : {}", filteredItem));
    }

    /**
     * This method demonstrates the map operation on a Flux by applying function on flux stream.
     */
    private void demoMapOperation() {
        Flux.just(1, 2, 3)
                .map( x -> x * 10)
                .subscribe( fn -> log.info("fn : {}", fn));
    }

    /**
     * This method demonstrates the FlatMap operation by flattening two flux sources.
     * Note tha flatmap does not garnette the order of the results.
     */
    private void demoFlatMapOperation() {
        // This flux emits 5 strings
        Flux.just("a", "b", "c", "d", "e")
                .flatMap( i -> {
                    final int delayFactor = new Random().nextInt(5);
                    // here we have another flux which emits 5 items, with a random delay
                    // like a typical HTTP response from remote system.
                    return Flux.just(i + "x").delayElements(Duration.ofMillis(delayFactor * 1000));
                })
                .doOnNext( i -> {
                    log.info("fnFlatMap : {}", i);
                    // note the use of count down latch, if we do not use it the
                    // program will exit before the asynchronous events happen.
                    countDownLatch.countDown();
                })
                .subscribe();
    }

    /**
     * This method demonstrates the FlatMap operation by concatenating two flux sources.
     * Note tha concat map garnette the order of the results.
     * But it waits for the each observable to finish all the work until next one processed.
     */
    private void demoConcatMap() {
        Flux.just("a", "b", "c", "d", "e")
                .concatMap( i -> {
                    final int delayFactor = new Random().nextInt(5);
                    return Flux.just(i + "x").delayElements(Duration.ofMillis(delayFactor * 1000));
                }).doOnNext( i -> {
                    log.info("fnConcatMap : {}", i);
                    countDownLatch.countDown();
                }).subscribe();
    }



    public static void main(String [] args) throws InterruptedException {
        FluxDemo fluxDemo = new FluxDemo();

        fluxDemo.stopWatch.start();
        //fluxDemo.demoFilterOperation();

        //fluxDemo.demoMapOperation();

        fluxDemo.demoFlatMapOperation();

        //fluxDemo.demoConcatMap();

        fluxDemo.countDownLatch.await();

        fluxDemo.stopWatch.stop();

        log.info("Time for execution : {}", fluxDemo.stopWatch.getTime());
    }
}
