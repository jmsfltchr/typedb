/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package hypergraph.graph.vertex;

import hypergraph.graph.edge.EdgeMapImpl;
import hypergraph.graph.util.Schema;
import hypergraph.graph.edge.Edge;

import java.util.Arrays;

public abstract class Vertex<
        VERTEX_SCHEMA extends Schema.Vertex,
        VERTEX extends Vertex,
        EDGE_SCHEMA extends Schema.Edge,
        EDGE extends Edge<EDGE_SCHEMA, VERTEX>,
        VERTEX_ITER extends EdgeMapImpl.VertexIteratorBuilder<VERTEX, EDGE>> {

    protected final VERTEX_SCHEMA schema;

    protected final EdgeMapImpl<VERTEX, EDGE_SCHEMA, EDGE, VERTEX_ITER> outs;
    protected final EdgeMapImpl<VERTEX, EDGE_SCHEMA, EDGE, VERTEX_ITER> ins;

    protected byte[] iid;

    Vertex(byte[] iid, VERTEX_SCHEMA schema) {
        this.schema = schema;
        this.iid = iid;
        outs = newEdgeMap(EdgeMapImpl.Direction.OUT);
        ins = newEdgeMap(EdgeMapImpl.Direction.IN);
    }

    protected abstract EdgeMapImpl<VERTEX, EDGE_SCHEMA, EDGE, VERTEX_ITER> newEdgeMap(EdgeMapImpl.Direction direction);

    public abstract Schema.Status status();

    public VERTEX_SCHEMA schema() {
        return schema;
    }

    public abstract void commit();

    public abstract void delete();

    public EdgeMapImpl<VERTEX, EDGE_SCHEMA, EDGE, VERTEX_ITER> outs() {
        return outs;
    }

    public EdgeMapImpl<VERTEX, EDGE_SCHEMA, EDGE, VERTEX_ITER> ins() {
        return ins;
    }

    public byte[] iid() {
        return iid;
    }

    public void iid(byte[] iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName() + ": [" + schema + "] " + Arrays.toString(iid);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Vertex that = (Vertex) object;
        return Arrays.equals(this.iid, that.iid);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(iid);
    }

}
