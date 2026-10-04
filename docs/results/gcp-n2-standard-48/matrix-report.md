# compio-pool vs tokio: compio-bench-48, 2026-10-03T18:27:26+00:00

* CPU: Intel(R) Xeon(R) CPU @ 2.80GHz; 48 CPUs = 24 physical cores × 2 thread(s); 2 NUMA node(s)
* Memory: `Mem:          193399        2186      189763           1        2623      191213`; virtualization: google; kernel 7.0.0-1011-gcp
* io_uring_disabled=0, governor=n/a, somaxconn=4096, tcp_tw_reuse=2, nofile=[1048576, 1048576]
* Cache (cpu0): L1 Data 32K, L1 Instruction 32K, L2 Unified 1024K, L3 Unified 33792K
* rustc 1.99.0 (b940084d7 2026-09-28); git reuseport-workers@58ed11f (uncommitted changes)
* Each cell is the median of 3 runs of 5 s, repeats interleaved across servers; ±N% is half the min-max spread. Split placement uses up to 13 dedicated server cores.

Legend: **S** the server's CPUs were at least 85% busy, so the server is the bottleneck. **C** the client's CPUs were at least 85% busy, so the number mostly measures the load generator and is a lower bound. **B** the whole machine was at least 90% busy (shared placement). No flag: neither side was saturated, so throughput is limited by the closed loop (connections ÷ latency).

## scale512

Worker scaling, 512 B echo. Server on W dedicated cores, clients on the others.

Server CPUs `0`, client CPUs `1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47`.

|  | tokio default | tokio per-core | compio-pool | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|
| W=1 | 103,887 [S] ±2% | 105,782 [S] ±0% | 109,325 [S] ±2% | 1.02× | 1.03× |
| W=2 | 192,918 [S] ±1% | 201,023 [S] ±1% | 198,066 [S] ±1% | 1.04× | 0.99× |
| W=4 | 373,144 [S] ±0% | 410,720 [S] ±1% | 392,283 [S] ±2% | 1.10× | 0.96× |
| W=8 | 771,297 [S] ±11% | 873,304 [S] ±0% | 850,942 [S] ±0% | 1.13× | 0.97× |
| W=13 | 854,021 ±2% | 1,419,280 [SC] ±0% | 1,347,233 [SC] ±0% | 1.66× | 0.95× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| W=1 | 623 / 658 | 603 / 635 | 580 / 639 |
| W=2 | 333 / 492 | 318 / 362 | 321 / 345 |
| W=4 | 342 / 578 | 297 / 409 | 312 / 406 |
| W=8 | 320 / 655 | 281 / 403 | 277 / 406 |
| W=13 | 359 / 883 | 284 / 406 | 312 / 418 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| W=1 | 9.6, 12.3 | 9.4, 12.5 | 9.1, 9.7 |
| W=2 | 10.1, 12.7 | 9.9, 12.4 | 10.1, 12.0 |
| W=4 | 10.1, 13.6 | 9.7, 13.5 | 10.2, 13.5 |
| W=8 | 9.6, 14.1 | 9.1, 13.9 | 9.3, 13.5 |
| W=13 | 10.2, 16.6 | 9.1, 14.7 | 9.6, 14.9 |

## scale16k

Worker scaling, 16 KiB echo. Server on W dedicated cores, clients on the others.

Server CPUs `0`, client CPUs `1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47`.

|  | tokio default | tokio per-core | compio-pool | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|
| W=1 | 68,024 [S] ±4% | 70,423 [S] ±2% | 67,714 [S] ±1% | 1.04× | 0.96× |
| W=2 | 128,253 [S] ±0% | 134,278 [S] ±0% | 132,052 [S] ±2% | 1.05× | 0.98× |
| W=4 | 253,952 [S] ±0% | 276,867 [S] ±0% | 268,634 [S] ±0% | 1.09× | 0.97× |
| W=8 | 418,232 ±0% | 584,187 [S] ±1% | 575,684 [S] ±1% | 1.40× | 0.99× |
| W=13 | 547,481 ±5% | 877,689 [SC] ±0% | 836,755 [SC] ±1% | 1.60× | 0.95× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| W=1 | 950 / 998 | 900 / 1,167 | 935 / 1,069 |
| W=2 | 501 / 744 | 488 / 545 | 479 / 545 |
| W=4 | 502 / 863 | 459 / 503 | 377 / 677 |
| W=8 | 470 / 972 | 451 / 540 | 448 / 574 |
| W=13 | 564 / 1,407 | 484 / 674 | 493 / 718 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| W=1 | 14.6, 18.9 | 14.1, 17.9 | 14.7, 15.6 |
| W=2 | 15.4, 19.6 | 14.8, 19.3 | 15.1, 17.4 |
| W=4 | 15.2, 20.8 | 14.4, 20.9 | 14.8, 19.1 |
| W=8 | 14.4, 20.9 | 13.6, 20.8 | 13.8, 19.3 |
| W=13 | 16.5, 26.6 | 14.7, 24.7 | 15.4, 24.4 |

## payload

Payload sweep at a fixed worker count. Server and clients on separate cores.

Server CPUs `0,1,2,3`, client CPUs `4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47`; 4 worker(s).

|  | tokio default | tokio per-core | compio-pool | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|
| 64 B | 380,673 [S] ±1% | 411,057 [S] ±1% | 396,609 [S] ±0% | 1.08× | 0.96× |
| 512 B | 365,635 [S] ±1% | 420,888 [S] ±2% | 392,891 [S] ±0% | 1.15× | 0.93× |
| 2048 B | 347,701 [S] ±0% | 393,361 [S] ±2% | 366,844 [S] ±1% | 1.13× | 0.93× |
| 8192 B | 302,413 [S] ±0% | 333,699 [S] ±1% | 316,183 [S] ±0% | 1.10× | 0.95× |
| 16384 B | 254,570 [S] ±1% | 278,942 [S] ±1% | 269,117 [S] ±0% | 1.10× | 0.96× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 64 B | 334 / 567 | 285 / 444 | 317 / 370 |
| 512 B | 349 / 587 | 320 / 394 | 340 / 375 |
| 2048 B | 366 / 619 | 336 / 459 | 355 / 391 |
| 8192 B | 421 / 721 | 371 / 466 | 407 / 454 |
| 16384 B | 500 / 871 | 463 / 573 | 453 / 582 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 64 B | 9.9, 13.3 | 9.7, 13.3 | 10.0, 13.1 |
| 512 B | 10.3, 13.8 | 9.5, 13.3 | 10.1, 13.3 |
| 2048 B | 10.9, 14.8 | 10.1, 14.1 | 10.8, 14.1 |
| 8192 B | 12.6, 17.3 | 11.9, 17.2 | 12.6, 16.2 |
| 16384 B | 15.1, 20.8 | 14.3, 20.4 | 14.8, 19.2 |

## conns

Connection-count sweep at a fixed worker count. Server and clients on separate cores. Fewer connections than workers leaves workers idle by `SO_REUSEPORT` hash.

Server CPUs `0,1,2,3`, client CPUs `4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47`; 4 worker(s).

|  | tokio default | tokio per-core | compio-pool | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|
| 4 conns | 133,064 ±2% | 164,215 ±0% | 159,067 ±3% | 1.23× | 0.97× |
| 16 conns | 332,786 [S] ±0% | 342,855 [S] ±5% | 331,306 [S] ±4% | 1.03× | 0.97× |
| 64 conns | 357,694 [S] ±1% | 406,749 [S] ±1% | 394,750 [S] ±0% | 1.14× | 0.97× |
| 256 conns | 369,980 [S] ±12% | 428,629 [S] ±1% | 392,255 [S] ±1% | 1.16× | 0.92× |
| 1024 conns | 314,746 ±13% | 410,157 [S] ±3% | 389,385 [S] ±2% | 1.30× | 0.95× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 4 conns | 30 / 45 | 25 / 34 | 25 / 36 |
| 16 conns | 48 / 80 | 38 / 94 | 50 / 86 |
| 64 conns | 176 / 303 | 142 / 241 | 160 / 198 |
| 256 conns | 688 / 1,157 | 588 / 735 | 624 / 789 |
| 1024 conns | 2,158 / 5,439 | 2,523 / 2,702 | 2,570 / 2,810 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 4 conns | 15.0, 9.7 | 11.8, 11.0 | 12.9, 11.2 |
| 16 conns | 11.0, 10.7 | 10.4, 11.5 | 10.9, 11.3 |
| 64 conns | 10.3, 13.4 | 9.8, 13.4 | 10.1, 13.0 |
| 256 conns | 10.3, 14.2 | 9.3, 13.3 | 10.1, 13.8 |
| 1024 conns | 10.0, 14.6 | 9.6, 14.4 | 10.1, 14.2 |

## shared

Nothing separated: the clients float over every CPU, the servers pin themselves. What plain `cargo run` of both programs gives.

Server CPUs `unpinned`, client CPUs `unpinned`.

|  | tokio default | tokio per-core | compio-pool | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|
| 512 B, W=1 | 152,061 ±0% | 156,627 ±1% | 155,944 ±2% | 1.03× | 1.00× |
| 512 B, W=2 | 224,469 ±1% | 292,570 ±6% | 288,130 ±2% | 1.30× | 0.98× |
| 512 B, W=4 | 459,857 ±6% | 485,842 ±2% | 436,065 ±0% | 1.06× | 0.90× |
| 512 B, W=8 | 591,513 ±15% | 725,523 ±0% | 701,938 ±3% | 1.23× | 0.97× |
| 512 B, W=16 | 923,220 ±13% | 1,392,464 ±2% | 1,311,851 ±1% | 1.51× | 0.94× |
| 512 B, W=24 | 1,069,035 ±0% | 2,070,034 [B] ±5% | 2,019,740 [B] ±1% | 1.94× | 0.98× |
| 16384 B, W=1 | 97,323 ±5% | 99,288 ±2% | 97,770 ±3% | 1.02× | 0.98× |
| 16384 B, W=2 | 155,340 ±1% | 206,369 ±1% | 199,872 ±1% | 1.33× | 0.97× |
| 16384 B, W=4 | 311,797 ±6% | 346,677 ±2% | 317,912 ±1% | 1.11× | 0.92× |
| 16384 B, W=8 | 422,678 ±11% | 514,846 ±1% | 528,667 ±2% | 1.22× | 1.03× |
| 16384 B, W=16 | 614,739 ±12% | 872,810 ±2% | 853,357 ±1% | 1.42× | 0.98× |
| 16384 B, W=24 | 804,506 ±2% | 1,284,749 [B] ±2% | 1,242,595 [B] ±3% | 1.60× | 0.97× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 512 B, W=1 | 425 / 458 | 406 / 433 | 406 / 450 |
| 512 B, W=2 | 288 / 449 | 199 / 290 | 223 / 245 |
| 512 B, W=4 | 271 / 528 | 259 / 349 | 272 / 387 |
| 512 B, W=8 | 329 / 712 | 345 / 460 | 353 / 484 |
| 512 B, W=16 | 416 / 1,011 | 352 / 534 | 385 / 554 |
| 512 B, W=24 | 514 / 1,313 | 369 / 608 | 372 / 585 |
| 16384 B, W=1 | 663 / 715 | 638 / 717 | 647 / 772 |
| 16384 B, W=2 | 415 / 636 | 308 / 341 | 271 / 420 |
| 16384 B, W=4 | 402 / 776 | 364 / 438 | 365 / 557 |
| 16384 B, W=8 | 464 / 1,007 | 491 / 614 | 489 / 676 |
| 16384 B, W=16 | 629 / 1,434 | 613 / 792 | 586 / 1,055 |
| 16384 B, W=24 | 746 / 1,984 | 559 / 1,706 | 627 / 872 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 512 B, W=1 | 6.6, 7.5 | 6.4, 7.6 | 6.4, 6.8 |
| 512 B, W=2 | 8.7, 9.2 | 6.8, 7.5 | 6.9, 7.5 |
| 512 B, W=4 | 8.3, 10.0 | 8.2, 9.9 | 9.1, 9.0 |
| 512 B, W=8 | 10.3, 13.0 | 11.0, 12.5 | 11.3, 12.5 |
| 512 B, W=16 | 13.0, 14.6 | 11.2, 12.5 | 12.0, 13.2 |
| 512 B, W=24 | 13.7, 16.3 | 10.7, 11.4 | 11.1, 11.7 |
| 16384 B, W=1 | 10.2, 11.6 | 10.0, 10.9 | 10.2, 9.1 |
| 16384 B, W=2 | 12.6, 12.7 | 9.7, 10.7 | 10.0, 9.1 |
| 16384 B, W=4 | 12.4, 13.2 | 11.5, 12.6 | 12.5, 13.0 |
| 16384 B, W=8 | 14.6, 18.0 | 15.4, 19.1 | 14.9, 15.5 |
| 16384 B, W=16 | 20.1, 20.9 | 18.1, 21.3 | 18.6, 19.9 |
| 16384 B, W=24 | 20.4, 22.7 | 17.5, 18.7 | 18.7, 19.1 |

## churn

A fresh connection per request (`--reconnect 1`), so each request is connect, hash, accept, serve, close. `cap=2` and `cap=1` force most connections through compio-pool's detach, channel and attach handoff.

Server CPUs `0,1,2,3`, client CPUs `4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47`; 4 worker(s).

|  | tokio default | tokio per-core | compio-pool | compio-pool cap=2 | compio-pool cap=1 | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|---|---|
| reconnect per request | 19,867 [C] ±0% | 20,196 [C] ±0% | 20,254 [C] ±0% | 16,960 ±0% | 15,656 ±0% | 1.02× | 1.00× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool | compio-pool cap=2 | compio-pool cap=1 |
|---|---|---|---|---|---|
| reconnect per request | 795 / 15,997 | 617 / 17,440 | 672 / 17,854 | 915 / 18,973 | 1,358 / 29,863 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool | compio-pool cap=2 | compio-pool cap=1 |
|---|---|---|---|---|---|
| reconnect per request | 41.8, 1767.1 | 33.4, 1864.7 | 34.2, 1828.8 | 38.9, 423.2 | 44.7, 218.9 |

compio-pool counters at the end of the median run:

* compio-pool: `active 0 queued 0/4096 accepted 101333 local 101333 handed_off 0 claimed 0 bounced 0 oversub 0 rejected 0 done 101333 errs 0`
* compio-pool cap=2: `active 0 queued 0/4096 accepted 84867 local 12481 handed_off 72386 claimed 72386 bounced 3 oversub 0 rejected 0 done 84867 errs 0`
* compio-pool cap=1: `active 0 queued 0/4096 accepted 78344 local 4905 handed_off 73439 claimed 73439 bounced 2 oversub 0 rejected 0 done 78344 errs 0`

## numa

Server on one NUMA node; clients on the same node against clients on another.

Server CPUs `0,1,2,3`, client CPUs `4,5,6,7,8,9,10,11,28,29,30,31,32,33,34,35`; 4 worker(s).

|  | tokio default | tokio per-core | compio-pool | per-core ÷ default | compio ÷ per-core |
|---|---|---|---|---|---|
| clients on the server's node | 559,209 [S] ±0% | 625,000 [S] ±0% | 607,100 [S] ±0% | 1.12× | 0.97× |
| clients on another node | 365,473 [S] ±0% | 402,882 [S] ±0% | 390,786 [S] ±0% | 1.10× | 0.97× |

p50 / p99 latency, µs:

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| clients on the server's node | 226 / 407 | 210 / 265 | 212 / 240 |
| clients on another node | 348 / 586 | 330 / 398 | 297 / 460 |

CPU per request, µs (server user+system, client):

|  | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| clients on the server's node | 6.8, 10.4 | 6.4, 10.3 | 6.6, 9.8 |
| clients on another node | 10.3, 13.9 | 9.9, 13.8 | 10.2, 13.5 |

