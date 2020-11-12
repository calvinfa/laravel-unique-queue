<?php

namespace Mlntn\Queue;

use Illuminate\Queue\LuaScripts;
use Illuminate\Queue\Jobs\RedisJob;
use Laravel\Horizon\RedisQueue;

class HorizonUniqueQueue extends RedisQueue
{

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string  $queue
     * @param  array   $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $connection = $this->getConnection();
        $data = json_decode($payload, true);

        $tracker = $this->getTrackerName($queue) . ":" . $data['uniqueIdentifier'];

        $exists = $connection->exists($tracker);

        if ($exists) {
            return null;
        }

        if (parent::pushRaw($payload, $queue, $options)) {
            $connection->set($tracker, $data['id'], "ex", 8 * 60 * 60);
        }
    }

    /**
     * Create a payload for an object-based queue handler.
     *
     * @param  mixed  $job
     * @return array
     */
    protected function createObjectPayload($job,$queue)
    {
        return array_merge([
            'uniqueIdentifier' => $job->getUniqueIdentifier(),
        ], parent::createObjectPayload($job,$queue));
    }

    /**
     * Delete a reserved job from the queue.
     *
     * @param  string  $queue
     * @param  RedisJob  $job
     * @return void
     */
    public function deleteReserved($queue, $job)
    {
        parent::deleteReserved($queue, $job);

        $data = json_decode($job->getRawBody(), true);

        $this->getConnection()->del($this->getTrackerName($queue) . ":" . $data['uniqueIdentifier']);
    }

    protected function getTrackerName($queue)
    {
        return $this->getQueue($queue).':tracker';
    }

}
