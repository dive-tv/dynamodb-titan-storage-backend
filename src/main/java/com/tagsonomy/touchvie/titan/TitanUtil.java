/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.tagsonomy.touchvie.titan;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.amazon.titan.diskstorage.dynamodb.Client;
import com.amazon.titan.diskstorage.dynamodb.DynamoDBDelegate;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class TitanUtil {

	private final Configuration graphConfig;

	public TitanUtil(String graphConfigPath) {
		this.graphConfig = loadProperties(graphConfigPath);
	}

	public Client createClient() {
		return new Client(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
				new CommonsConfiguration(this.graphConfig), Restriction.NONE));
	}

	public TitanGraph openGraph() {
		return TitanFactory.open(this.graphConfig);
	}

	/*
	 * public TitanGraph openGraphWithElasticSearch(BackendDataModel
	 * backendDataModel) { Configuration config =
	 * createTestGraphConfig(backendDataModel); addElasticSearchConfig(config);
	 * TitanGraph graph = TitanFactory.open(config); return graph; }
	 */

	public void tearDownGraph(TitanGraph graph) throws BackendException {
		if (null != graph) {
			graph.close();
		}
		cleanUpTables();
	}

	public Configuration loadProperties(String configFilePath) {

		File propertiesFile = new File(configFilePath);

		Preconditions.checkArgument(propertiesFile.exists());
		Preconditions.checkArgument(propertiesFile.isFile());
		Preconditions.checkArgument(propertiesFile.canRead());

		PropertiesConfiguration storageConfig;
		try {
			storageConfig = new PropertiesConfiguration(propertiesFile);
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
		return storageConfig;
	}

	private void deleteAllTables(String prefix, DynamoDBDelegate delegate) throws BackendException {
		final ListTablesResult result = delegate.listAllTables();
		for (String tableName : result.getTableNames()) {
			if (prefix != null && !tableName.startsWith(prefix)) {
				continue;
			}
			try {
				delegate.deleteTable(new DeleteTableRequest().withTableName(tableName));
			} catch (ResourceNotFoundException e) {
			}
		}
	}

	private void cleanUpTables() throws BackendException {
		final Client client = createClient();
		String prefix = this.graphConfig.getString("storage.dynamodb.prefix", null);
		deleteAllTables(prefix, client.delegate());
		client.delegate().shutdown();
	}
}
