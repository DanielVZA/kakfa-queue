<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer;
    protected $producer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAKFA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(env("FLUSH_TIME", 1000));
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
    }

    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);

        try {
            $message = $this->consumer->consume(120 * 1000);

            switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $job = unserialize($message->payload);
                $job->handle();
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                print_r("\e[0;30;46mInfo: No more messages; will wait for more\e[0m\n");
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                print_r("\e[0;30;41mError: Timed out\e[0m\n");
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
                break;
            }
        } catch (\Exception $e) {
            print_r($e->getMessage());
        }
    }
}
