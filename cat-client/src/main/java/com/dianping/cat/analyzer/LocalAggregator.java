/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dianping.cat.analyzer;

import java.util.List;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.util.Threads.Task;


//本地数据聚合
public class LocalAggregator {

	public static void aggregate(MessageTree tree) {
		analyzerProcessTree(tree);
	}

	private static void analyzerProcessTree(MessageTree tree) {
		Message message = tree.getMessage();

		if (message instanceof Transaction) {//transaction
			analyzerProcessTransaction((Transaction) message);
		} else if (message instanceof Event) {//event
			EventAggregator.getInstance().logEvent((Event) message);
		}
	}

	private static void analyzerProcessTransaction(Transaction transaction) {
		//记录transaction
		TransactionAggregator.getInstance().logTransaction(transaction);
		List<Message> child = transaction.getChildren();

		for (Message message : child) {
			if (message instanceof Transaction) {
				analyzerProcessTransaction((Transaction) message);
			} else if (message instanceof Event) {
				//记录event
				EventAggregator.getInstance().logEvent((Event) message);
			}
		}
	}

	public static class DataUploader implements Task {

		private boolean m_active = true;

		@Override
		public String getName() {
			return "local-data-aggregator";
		}

		@Override
		public void run() {
			while (m_active) {//1s 一次
				long start = System.currentTimeMillis();

				try {
					//发送transaction数据
					TransactionAggregator.getInstance().sendTransactionData();
					//发送event数据
					EventAggregator.getInstance().sendEventData();
				} catch (Exception ex) {
					Cat.logError(ex);
				}

				long duration = System.currentTimeMillis() - start;

				if (duration >= 0 && duration < 1000) {
					try {
						Thread.sleep(1000 - duration);
					} catch (InterruptedException e) {
						break;
					}
				}
			}
		}

		@Override
		public void shutdown() {
			m_active = false;
		}
	}

}
