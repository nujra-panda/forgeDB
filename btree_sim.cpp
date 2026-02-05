/**
 * In-Memory B+ Tree Simulation & Visualizer
 * * Capabilities:
 * 1. Implements Order-3 B+ Tree logic (Split, Promote).
 * 2. Generates a 'btree_viz.html' file automatically.
 */

#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>
#include <fstream>
#include <string>

// --- CONFIGURATION ---
constexpr int ORDER = 3;

// --- NODE DEFINITIONS ---
enum class NodeType { Internal, Leaf };

struct Node {
    NodeType type;
    std::vector<int> keys;
    Node* parent;
    int id; // Unique ID for visualization

    Node(NodeType t) : type(t), parent(nullptr) {
        static int _id_counter = 0;
        id = _id_counter++;
    }
    virtual ~Node() = default;
};

struct InternalNode : Node {
    std::vector<Node*> children;
    InternalNode() : Node(NodeType::Internal) {}
};

struct LeafNode : Node {
    LeafNode* next_leaf;
    LeafNode() : Node(NodeType::Leaf), next_leaf(nullptr) {}
};

// --- B+ TREE CLASS ---
class BPlusTree {
public:
    BPlusTree() { root = new LeafNode(); }
    
    // --- INSERTION LOGIC ---
    void insert(int key) {
        LeafNode* leaf = find_leaf(root, key);
        auto it = std::upper_bound(leaf->keys.begin(), leaf->keys.end(), key);
        leaf->keys.insert(it, key);

        if (leaf->keys.size() > ORDER) {
            split_leaf(leaf);
        }
    }

    // --- VISUALIZATION ENGINE ---
    void generate_html_report(std::string filename) {
        std::ofstream outfile(filename);
        
        // 1. Write HTML Header & CSS (Using custom delimiter "HTML")
        outfile << R"HTML(
<!DOCTYPE html>
<html>
<head>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body { font-family: sans-serif; background: #f4f4f9; display: flex; flex-direction: column; align-items: center; }
    h2 { color: #333; }
    .node rect { fill: #fff; stroke: #333; stroke-width: 2px; rx: 5; ry: 5; }
    .node text { font: 14px sans-serif; text-anchor: middle; dominant-baseline: middle; }
    .node-internal rect { stroke: #2196F3; fill: #E3F2FD; }
    .node-leaf rect { stroke: #4CAF50; fill: #E8F5E9; }
    .link { fill: none; stroke: #ccc; stroke-width: 2px; }
  </style>
</head>
<body>
  <h2>B+ Tree Structure (Order 3)</h2>
  <div id="tree-container"></div>
  <script>
    const treeData = )HTML";

        // 2. Dump Tree as JSON
        dump_node_json(root, outfile);

        // 3. Write D3.js Visualization Script (Using custom delimiter "HTML")
        outfile << R"HTML(;

    // Set dimensions
    const margin = {top: 40, right: 90, bottom: 50, left: 90},
          width = 1200 - margin.left - margin.right,
          height = 600 - margin.top - margin.bottom;

    const svg = d3.select("#tree-container").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    const treemap = d3.tree().size([width, height]);
    let root = d3.hierarchy(treeData);
    
    // Assign sizes based on content
    root.descendants().forEach(d => {
       d.data.width = (d.data.keys.length * 25) + 20; 
       d.data.height = 30;
    });

    const nodes = treemap(root);

    // Links
    svg.selectAll(".link")
        .data(nodes.links())
      .enter().append("path")
        .attr("class", "link")
        .attr("d", d => {
           return "M" + d.source.x + "," + d.source.y
             + "C" + d.source.x + "," + (d.source.y + d.target.y) / 2
             + " " + d.target.x + "," + (d.source.y + d.target.y) / 2
             + " " + d.target.x + "," + d.target.y;
           });

    // Nodes
    const node = svg.selectAll(".node")
        .data(nodes.descendants())
      .enter().append("g")
        .attr("class", d => "node " + (d.children ? "node-internal" : "node-leaf"))
        .attr("transform", d => "translate(" + d.x + "," + d.y + ")");

    // Node Box
    node.append("rect")
        .attr("width", d => Math.max(40, d.data.keys.length * 20 + 20))
        .attr("height", 30)
        .attr("x", d => -(Math.max(40, d.data.keys.length * 20 + 20)) / 2)
        .attr("y", -15);

    // Node Text (Keys)
    node.append("text")
        .text(d => d.data.keys.join(" | "));

  </script>
</body>
</html>
)HTML";
        outfile.close();
        std::cout << "Visualization saved to 'btree_viz.html'\n";
    }

private:
    Node* root;

    // --- JSON DUMPER ---
    void dump_node_json(Node* node, std::ostream& out) {
        if (!node) return;
        out << "{";
        out << "\"type\": \"" << (node->type == NodeType::Internal ? "Internal" : "Leaf") << "\",";
        out << "\"keys\": [";
        for (size_t i = 0; i < node->keys.size(); ++i) {
            out << node->keys[i] << (i < node->keys.size() - 1 ? "," : "");
        }
        out << "]";

        if (node->type == NodeType::Internal) {
            out << ", \"children\": [";
            InternalNode* internal = static_cast<InternalNode*>(node);
            for (size_t i = 0; i < internal->children.size(); ++i) {
                dump_node_json(internal->children[i], out);
                if (i < internal->children.size() - 1) out << ",";
            }
            out << "]";
        }
        out << "}";
    }

    // --- HELPERS ---
    LeafNode* find_leaf(Node* node, int key) {
        if (node->type == NodeType::Leaf) return static_cast<LeafNode*>(node);
        InternalNode* internal = static_cast<InternalNode*>(node);
        size_t i = 0;
        while (i < internal->keys.size() && key >= internal->keys[i]) i++;
        return find_leaf(internal->children[i], key);
    }

    void insert_into_parent(Node* left, int key, Node* right) {
        if (left->parent == nullptr) {
            create_new_root(left, key, right);
            return;
        }
        InternalNode* parent = static_cast<InternalNode*>(left->parent);
        auto it = std::upper_bound(parent->keys.begin(), parent->keys.end(), key);
        size_t index = std::distance(parent->keys.begin(), it);
        parent->keys.insert(it, key);
        parent->children.insert(parent->children.begin() + index + 1, right);
        right->parent = parent;
        if (parent->keys.size() > ORDER) split_internal(parent);
    }

    void split_leaf(LeafNode* left) {
        LeafNode* right = new LeafNode();
        size_t split_index = (left->keys.size() + 1) / 2;
        right->keys.assign(left->keys.begin() + split_index, left->keys.end());
        left->keys.resize(split_index);
        right->next_leaf = left->next_leaf;
        left->next_leaf = right;
        int promote_key = right->keys.front();
        insert_into_parent(left, promote_key, right);
    }

    void split_internal(InternalNode* left) {
        InternalNode* right = new InternalNode();
        size_t mid_index = left->keys.size() / 2;
        int promote_key = left->keys[mid_index];
        right->keys.assign(left->keys.begin() + mid_index + 1, left->keys.end());
        right->children.assign(left->children.begin() + mid_index + 1, left->children.end());
        for (Node* child : right->children) child->parent = right;
        left->keys.resize(mid_index);
        left->children.resize(mid_index + 1);
        insert_into_parent(left, promote_key, right);
    }

    void create_new_root(Node* left, int key, Node* right) {
        InternalNode* new_root = new InternalNode();
        new_root->keys.push_back(key);
        new_root->children.push_back(left);
        new_root->children.push_back(right);
        left->parent = new_root;
        right->parent = new_root;
        root = new_root;
    }
};

int main() {
    BPlusTree tree;
    std::cout << "Running B+ Tree Simulation (Order " << ORDER << ")...\n";

    // Insert data 
    for (int i = 1; i <= 20; ++i) {
        tree.insert(i);
    }
    tree.insert(50);
    tree.insert(25);
    tree.insert(100);

    tree.generate_html_report("btree_viz.html");
    return 0;
}