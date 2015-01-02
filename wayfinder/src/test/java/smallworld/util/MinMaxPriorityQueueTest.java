package smallworld.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Range;


public class MinMaxPriorityQueueTest {
	
	private final static int CAPACITY = 5; 
	@SuppressWarnings("unused")
	private final MinMaxPriorityQueueTest GIVEN = this, AND = this, WITH = this, THEN = this, WHEN = this;
	final static MinMaxPriorityQueue<Integer> queue = MinMaxPriorityQueue.maximumSize(CAPACITY).create();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		queue.clear();
	}

	@Test
	public void test_insert_to_full_queue() {
		GIVEN.queue_is_full();
		AND.insert_to_queue(10);
		
		System.out.println(Arrays.toString(queue.toArray()));
		
		assertEquals(1, queue.peek().intValue());
		assertEquals(CAPACITY, queue.size());
		
		THEN.insert_to_queue(0);
		assertEquals(0, queue.peek().intValue());
		assertEquals(CAPACITY, queue.size());
	}

	private void insert_to_queue(int number) {
		queue.add(number);
	}
	
	private void insert_numbers_to_queue() {
		List<Integer> numbers = new ArrayList<Integer>(ContiguousSet.create(
				Range.closed(1, CAPACITY), DiscreteDomain.integers()).asList());
		Collections.shuffle(numbers);
		System.out.println(numbers);
		queue.addAll(numbers);
	}
	
	private void queue_is_full() {
		this.insert_numbers_to_queue();
		
		assertEquals(1, queue.peek().intValue());
		assertEquals(CAPACITY, queue.size());
	}
}
