/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.concurrent.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.function.Predicates;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.helpers.NamedThreadFactory.daemon;

@RunWith( Parameterized.class )
public class BoltSchedulerCausalClusterIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 );
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private ExecutorService executor;
    private Cluster cluster;
    private Driver driver;

    @Parameterized.Parameter
    public int runIndex;

    @Parameterized.Parameters
    public static List<Integer> getParameters()
    {
        return Arrays.asList( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 );
    }

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();
        driver = GraphDatabase.driver( cluster.getDbWithRole( Role.LEADER ).routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
        executor = newExecutor();
    }

    @After
    public void cleanup()
    {
        executor.shutdown();
        driver.close();
        cluster.shutdown();
    }

    @Test
    public void shouldNotFail() throws Exception
    {
        String label = "Person";
        String property = "name";
        String value = "Tony Stark";

        AtomicBoolean stop = new AtomicBoolean();
        executor = newExecutor();

        {
            List<CompletableFuture<?>> results = new ArrayList<>();

            // launch writers and readers that use transaction functions and thus should never fail
            for ( int i = 0; i < 3; i++ )
            {
                results.add( CompletableFuture.supplyAsync( readNodesCallable( driver, label, property, value, stop ), executor ) );
            }
            for ( int i = 0; i < 2; i++ )
            {
                results.add( CompletableFuture.supplyAsync( createNodesCallable( driver, label, property, value, stop ), executor ) );
            }

            // make connections throw while reads and writes are in progress
            long deadline = System.currentTimeMillis() + MINUTES.toMillis( 1 );
            Predicates.await( () -> System.currentTimeMillis() > deadline, 5, MINUTES );
            stop.set( true );

            CompletableFuture<Void> combined = CompletableFuture.allOf( results.toArray( new CompletableFuture[0] ) );
            combined.get( 5, MINUTES );

            //Futures.combine( results );
            assertThat( countNodes( driver.session(), label, property, value ), greaterThan( 0 ) ); // some nodes should be created
        }
    }

    private static Supplier<Void> createNodesCallable( Driver driver, String label, String property, String value, AtomicBoolean stop )
    {
        return () ->
        {
            long interval = SECONDS.toMillis( 10 );
            long lastInducedError = System.currentTimeMillis();

            while ( !stop.get() )
            {
                boolean fireError = false;
                long now = System.currentTimeMillis();
                if ( now - lastInducedError > interval )
                {
                    fireError = true;
                    lastInducedError = now;
                }

                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    createNode( session, label, property, value, fireError );
                }
                catch ( Throwable t )
                {
                    stop.set( true );
                    throw t;
                }
            }
            return null;
        };
    }

    private static Supplier<Void> readNodesCallable( Driver driver, String label, String property, String value, AtomicBoolean stop )
    {
        return () ->
        {
            long interval = SECONDS.toMillis( 10 );
            long lastInducedError = System.currentTimeMillis();
            while ( !stop.get() )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    boolean fireError = false;
                    long now = System.currentTimeMillis();
                    if ( now - lastInducedError > interval )
                    {
                        fireError = true;
                        lastInducedError = now;
                    }

                    List<Long> ids = readNodeIds( session, label, property, value, fireError );
                    assertNotNull( ids );
                }
                catch ( Throwable t )
                {
                    stop.set( true );
                    throw t;
                }
            }
            return null;
        };
    }

    private static void createNode( Session session, String label, String property, String value, boolean fireError )
    {
        AtomicBoolean firedOnce = new AtomicBoolean( false );

        session.writeTransaction( tx ->
        {
            if ( fireError && !firedOnce.get() )
            {
                firedOnce.set( true );
                runCreateNode( tx, label, property, value, true );
            }
            else
            {
                runCreateNode( tx, label, property, value, false );
            }
            return null;
        } );
    }

    private static StatementResult runCreateNode( StatementRunner statementRunner, String label, String property, String value, boolean fireError )
    {
        if ( fireError )
        {
            throw new ServiceUnavailableException( "Unable to execute query" );
        }

        StatementResult result = statementRunner.run( "CREATE (n:" + label + ") SET n." + property + " = $value", parameters( "value", value ) );

        return result;
    }

    private static List<Long> readNodeIds( final Session session, final String label, final String property, final String value, boolean fireError )
    {
        AtomicBoolean firedOnce = new AtomicBoolean( false );
        return session.readTransaction( tx ->
        {
            if ( fireError && !firedOnce.get() )
            {
                firedOnce.set( true );
                throw new ServiceUnavailableException( "Unable to execute query" );
            }

            StatementResult result = tx.run( "MATCH (n:" + label + " {" + property + ": $value}) RETURN n LIMIT 10", parameters( "value", value ) );

            return result.list( record -> record.get( 0 ).asNode().id() );
        } );
    }

    private static int countNodes( Session session, String label, String property, String value )
    {
        return session.readTransaction( tx -> runCountNodes( tx, label, property, value ) );
    }

    private static int runCountNodes( StatementRunner statementRunner, String label, String property, String value )
    {
        StatementResult result = statementRunner.run( "MATCH (n:" + label + " {" + property + ": $value}) RETURN count(n)", parameters( "value", value ) );
        return result.single().get( 0 ).asInt();
    }

    private static ExecutorService newExecutor()
    {
        return Executors.newCachedThreadPool( daemon( BoltSchedulerCausalClusterIT.class.getSimpleName() + "-thread-" ) );
    }
}
