package org.opendaylight.distributed.tx.api;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

/**
 * Provider of distributed transactions.
 * Applications ask this provider for a distributed transactions over a specified set of nodes.
 *
 * Node is identified by its InstanceIdentifier and represented by a ReadWriteTransaction.
 * The node transaction can be provided by e.g. NETCONF Mountpoint
 */
public interface DTxProvider {

    /**
     * Instantiate a new distributed transaction for NETCONF.
     *
     * @param nodes set of instance IDs for nodes participating in a distributed tx.
     *
     * @return new distributed Tx for a set of nodes.
     * Per node transaction was successfully initialized for each node at this point.
     *
     * @throws DTxException.DTxInitializationFailedException if:
     * <ul>
     * <li> Unknown node was specified</li>
     * <li> Node is used by other distributed transaction</li>
     * <li> Node tx could not be initialized (node is in use by other client/is unreachable etc)</li>
     * </ul>
     */
    @Nonnull DTx newTx(@Nonnull Set<InstanceIdentifier<?>> nodes) throws DTxException.DTxInitializationFailedException;
    /**
     * Instantiate a new distributed transaction containing different providers.
     *
     * @param nodes maps of instance IDs for nodes participating from one or multiple providers in a distributed tx. Supported providers are
     * <ul>
     * <li> NETCONF_TX_PROVIDER </li>
     * <li>DATASTORE_TX_PROVIDER</li>
     * </ul>
     *
     * @return new distributed Tx for a set of nodes.
     * Per node transaction was successfully initialized for each node at this point.
     *
     * @throws DTxException.DTxInitializationFailedException if:
     * <ul>
     * <li> Unknown node was specified</li>
     * <li> Node is used by other distributed transaction</li>
     * <li> Node tx could not be initialized (node is in use by other client/is unreachable etc)</li>
     * </ul>
     */
    @Nonnull DTx newTx(@Nonnull Map<DTXLogicalTXProviderType, Set<InstanceIdentifier<?>>> nodes) throws DTxException.DTxInitializationFailedException;
}
