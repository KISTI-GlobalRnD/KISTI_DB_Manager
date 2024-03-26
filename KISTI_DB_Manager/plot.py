# KISTI_DB_Manager/plot.py
"""
Note
----
Made by Young Jin Kim (kimyoungjin06@gmail.com)
Last Update: 2024.03.15, YJ Kim

MariaDB/MySQL Handling for All type DB
To preprocess, import, export and manage the DB

# Updates
## 2024.03.17
- Add exception part for unstructured json branches
    - related: flatten_json_separate_lists, ..., flatten_json_separate_lists
    - except_keys: excepted part from unstructured json branches

Example
-------
"""
import pandas as pd
import numpy as np

__all__ = ["Cubic_Bezier", "draw_Bezier_edges", "get_position_grid", "init_vis_structure", 
          "plot_schema", "draw_schema"]

def Cubic_Bezier(p, o, ax, c=(0,0), r=0.1, color='black', lw=1.0, method='circular'):
    '''
    Draw Cubic Bezier curve from 3 points - Center, Parent, and Offspring

    Parameters
    ----------
    p : tuple of double; (x, y)
        a coordinate of Parent
    o : tuple of double; (x, y)
        a coordinate of Offspring
    c : tuple of double; (x, y)
        a coordinate of Center
    r : double
        Ratio for Bezier curve point
    color : str
        Color of edge

    Returns
    -------
    curve patch

    Examples
    --------
    >>> fig, ax = plt.subplots()
    >>> pat = Cubic_Bezier(P, O)
    >>> ax.add_patch(pat)
    >>> plt.show()
    '''

    # coordinate
    px, py = p
    ox, oy = o
    cx, cy = c

    # length
    if (c==p) & (method=='circular'):
        rl = 0
    else:
        rl = r * np.sqrt((px - ox)**2 + (py - oy)**2)
    
    # angular
    theta = np.arctan2((py - cy), (px - cx))
    phi = np.arctan2((oy - py), (ox - px)) - theta
    rho = np.arctan2((py - oy), (px - ox)) - np.arctan2((cy - oy), (cx - ox))
    omega = theta + phi - rho - np.pi

    rl_cp = rl * np.cos(phi)
    rl_cr = rl * np.cos(rho)
    
    # sub-coord
    x1 = px + rl_cp*np.cos(theta)
    y1 = py + rl_cp*np.sin(theta)
    x2 = ox + rl_cr*np.cos(omega)
    y2 = oy + rl_cr*np.sin(omega)

    if method == 'horizontal':
        dx = ox - px
        x1 = px + dx*r
        x2 = ox - dx*r
        y1 = py
        y2 = oy
    # plt.scatter(cx, cy, c='red', s=10)
    # plt.scatter(px, py, c='blue', s=1)
    # plt.scatter(x1, y1, c='grey')
    # plt.scatter(x2, y2, c='grey')

    # patch
    import matplotlib.path as mpath
    import matplotlib.patches as mpatches

    Path = mpath.Path
    pat = mpatches.PathPatch(
        Path([(px, py), (x1, y1), (x2, y2), (ox, oy)],
             [Path.MOVETO, Path.CURVE4, Path.CURVE4, Path.CURVE4]),
        fc="none", ec=color, lw=lw, transform=ax.transData)
    ax.add_patch(pat)
    return pat


def draw_Bezier_edges(
    G, pos, origin, ax,
    ratio=0.5, color='black', weight=False, w=1.0, method='circular'):
    '''
    Draw Cubic Bezier edges with Graph

    Parameters
    ----------
    G : nx.Graph
        a Graph
    pos : dict of position
        a coordination dict of G
    origin : str; node name of center
        the root of G
    ax : plt.ax
        ax
    r : float
        Ratio of edge
    color : str
        Color of edge

    Returns
    -------
    List of curve patch
    
    Example
    -------
    import networkx as nx

    G = nx.balanced_tree(5, 3)
    pos = nx.nx_agraph.graphviz_layout(G, prog="twopi", args="")
    origin = 0
    
    fig, ax = plt.subplots(figsize=(8, 8))
    egs = draw_Bezier_edges(
        G, pos, origin, ax,
        ratio=0.5, color='purple',
    )
    nx.draw_networkx_nodes(G, pos, node_size=12, node_color="red")
    plt.axis("equal")
    plt.show()
    '''
    C = pos[origin]
    egs = []
    for _data in G.edges(data=weight):
        if weight:
            p, o, w = _data
            w = w["weight"] 
        else:
            p, o = _data
            # w = 1.0
        P = pos[p]
        O = pos[o]
        eg = Cubic_Bezier(P, O, ax, C, ratio, color, w, method)
        egs.append(eg)
    return egs


def get_position_grid(types, origin, count_dict, depth=0, sep='__'):
    """
    Calculates the depth and count of hierarchical JSON structure elements for visualization.

    This function divides each element's unique key into hierarchical levels using a specified
    separator and accumulates the count of occurrences for each type across these levels.
    The output is a structured DataFrame summarizing the depth and count of elements at each level,
    facilitating their mapping to a visual grid for graphical representation.

    Parameters
    ----------
    types : pd.Series
        A pandas Series where the index represents unique keys of hierarchical elements and
        values represent their types.
    origin : str
        The root key from which to start the hierarchy.
    count_dict : dict
        A dictionary mapping each type to its count.
    depth : int, optional
        The initial depth of the hierarchy. Default is 0.
    sep : str, optional
        The separator used to divide the keys into hierarchical levels. Default is '__'.

    Returns
    -------
    pd.DataFrame
        A DataFrame grouped by hierarchical levels with summed counts of elements at each level.
    """
    def accum_count(uni_key, result, _type):
        present_key = ''
        for branch in uni_key.split(sep):
            present_key += branch
            try:
                result[present_key] += count_dict[_type]
            except:
                result[present_key] = count_dict[_type]
            present_key += sep
        return result
    
    _depth = [len(x.split(sep)) for x in types.index]
    _max_depth = max(_depth)
    _r = []
    unique_keys = types.index
    for unique_key in unique_keys:
        __r = unique_key.split(sep) 
        __r += ['']*(_max_depth-len(__r))
        _r.append(__r)
    cols = [f'rank_{x}' for x in range(1, _max_depth+1)]
    _res = pd.DataFrame(_r, columns=cols)
    _res['type'] = types.values
    _res['depth'] = _depth
    
    _cnt = {unique_keys[0].split(sep)[0]:0}
    
    for unique_key in unique_keys:
        accum_count(unique_key, _cnt, types[unique_key])
    _cnt = [_cnt[uk] for uk in unique_keys]
    _res['cnt'] = _cnt
    return _res.groupby(cols).sum().dropna()


def init_vis_structure(json_dict, G, origin, count_dict, forced={}, xy_unit=(14,1), sep='__', excepted=False):
    """
    Initializes the visualization structure for a given JSON dictionary by mapping it 
    onto a graph G, using positional information based on the hierarchical depth 
    and the number of sub-items.

    Parameters:
    - json_dict (dict): JSON dictionary to visualize.
    - G (networkx.DiGraph): A directed graph object from NetworkX where nodes and edges will be added.
    - origin (str): The root node's identifier in the graph.
    - count_dict (dict): A dictionary mapping node types to their counts for position calculation.
    - xy_unit (tuple): A tuple representing the unit distance in x (width) and y (height) directions between nodes (default is (14, 1)).
    - sep (str): Separator used to distinguish levels in the hierarchical structure (default is '__').

    Returns:
    - tuple: Returns a tuple containing the updated graph G, positions dictionary, labels dictionary, and types Series.
    """
    from .processing import json_to_key_pairs, key_pair_to_df#, select_duplicated

    
    key_pairs = json_to_key_pairs(json_dict, parent=origin, parent_type='Dict', sep=sep)
    # print(key_pairs)
    # When excepted, duplicated index exist
    # So, only revise Multiples delete singles    
    key_pairs_df = key_pair_to_df(key_pairs, sep=sep) # without unique_set, Multiple and Single is appear twice
    # print(key_pairs_df)
        
    types = key_pairs_df.set_index('branch')['type']
    # types = select_duplicated(types)
    # print(types)
    
    df_pos = get_position_grid(types, origin, count_dict)
    msk = df_pos['type'].str.contains('List')
    df_pos.loc[msk, 'cnt'] = 1
    msk = df_pos['type'].str.contains('Dict')
    df_pos.loc[msk, 'cnt'] = 0
    msk = df_pos['type'].str.contains('Value in')
    df_pos.loc[msk, 'cnt'] = 1
    df_pos['x'] = df_pos['depth']*xy_unit[0]
    df_pos['y'] = df_pos[::-1].cumsum()['cnt']*xy_unit[1]
    df_pos['name'] = [sep.join([_x for _x in x if _x != '']) for x in df_pos.index.values]
    # print(df_pos)
    # return df_pos
    # edges = key_pairs[['parent', 'branch']].dropna().values
    edges = np.array(key_pairs)[:,1:]
    G.add_edges_from(edges)
    pos = {n:(x, y) for n, x, y in df_pos[['name', 'x', 'y']].values}
    pos[origin] = (0,df_pos['y'].max())
    # print(pos)
    labels = {x:x.split('__')[-1] for x in list(G.nodes())}
    return G, pos, labels, types


def plot_schema(jsons, data_name="", origin='excepted', except_keys=[], forced={}, sep='__', 
          legend_loc='center left', node_size=12, font_size=3, X_SIZE=6, Y_SIZE=5, DPI=300):
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    import networkx as nx
    
    count_dict = {
            'Value':1, 'Value in List of Dict':1, 'List of Dict':0, 'Dict':0, 'List of Value':1, 'List':1
        }
    title = f'The structure of {data_name} XML'
    
    G = nx.Graph()
    G, pos, labels, types = init_vis_structure(jsons, G, origin, count_dict, forced, sep=sep)
    # G, pos, labels, types = init_vis_structure(json_ex, G, origin, count_dict, except_keys=except_keys)
    
    N_ROW = 1
    N_COL = 1
    plt.rcParams['font.family'] = ['NanumSquare', 'Helvetica']
    # plt.rcParams['font.family'] = ['Helvetica', 'NanumSquare']
    
    fig=plt.figure(figsize = (X_SIZE*N_COL,Y_SIZE*N_ROW), dpi=DPI)
    spec = gridspec.GridSpec(ncols=N_COL, nrows=N_ROW, figure=fig, )#, width_ratios=[1,1,.1], wspace=.3)
    axes = []
    axi=0
    ax = fig.add_subplot(spec[axi//N_COL,axi%N_COL]) # row, col
    
    # Draw Edge with Bezier
    draw_Bezier_edges(
        G, pos, origin, ax, w=.2,
        ratio=0.5, color='black', weight=False, method='horizontal')

    # Draw Nodes
    for i, key in enumerate(count_dict.keys()):
        nodelist = types[types == key].index
        if len(nodelist)> 0:
            msk = np.array([np.isin(except_keys, nl.split(sep)).any() for nl in nodelist])
            if origin != 'excepted':
                nodelist_excepted = nodelist[msk]
                collection_excepted = nx.draw_networkx_nodes(G, pos, nodelist=nodelist_excepted, node_size=node_size, label="__none",
                                                    node_color='none', alpha=1., ax=ax, edgecolors=f'C{i}',)
                collection_excepted.set_zorder(2.1)
                collection_excepted.set_linewidth(.4)
            nodelist_regularized = nodelist[~msk]
            collection_regularized = nx.draw_networkx_nodes(G, pos, nodelist=nodelist_regularized, node_size=node_size, label=key,
                                                node_color=f'C{i}', alpha=1., ax=ax, edgecolors='none')
            collection_regularized.set_zorder(2.1) # patch: 1, line: 2, text: 3
    if origin != 'excepted':
        _ecpt = ax.scatter([],[], s=node_size, label="Excepted", c='none', alpha=1., edgecolors=f'black',)
        _ecpt.set_zorder(2.1)
        _ecpt.set_linewidth(.4)
    _labels = nx.draw_networkx_labels(G, pos, labels=labels, 
                                      horizontalalignment='left', verticalalignment='bottom', 
                                      font_family='NanumSquare', 
                                      font_size=font_size, ax=ax)
    
    legend = plt.legend(scatterpoints=1, fontsize=font_size*1.5, loc='center left', title='branch type', title_fontsize=font_size*2.)
    for item in legend.legend_handles:
        item._sizes = [node_size]
    # plt.margins(x=.1)
    # plt.subplots_adjust(right=1.)
    plt.tight_layout()
    plt.axis('off')
    plt.title(title)
    return fig


def draw_schema(df_descs, flist, index_key, node_size=8, font_size=5, X_SIZE=10, Y_SIZE=24, 
                title='DB Schema', x_unit=1, table_unit=14, max_num_row=80, features=['Coverage', 'freq', 'uniq_ratio'], DPI=300, svg_fonttype='none', sep='__'):
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    import matplotlib.patches as mpatches
    from matplotlib.patches import Wedge
    
    def write_item():
        x = x_base
        y = y_base-i
        # name = idx.split(sep)[-1]
        
        name = " ".join(idx.split(sep)[-2:])
        type = df_desc.loc[idx, 'Type']
        fts = df_desc.loc[idx, features]
        freq = df_desc.loc[idx, 'freq']
        colors = ['lightsalmon', 'orchid', 'gold']
        color = 'royalblue'
        if freq == 1.: # freq
            alpha = 0.2
        else:
            alpha = 1.
            
        if name == index_key:
            color = 'limegreen'
        
        plt.scatter(x, y, alpha=alpha, s=node_size, c=color)
        plt.text(x+x_unit, y, name, alpha=alpha, ha='left', va='center', fontdict=dict(fontsize=font_size)) # Name
        plt.text(x+x_unit+table_unit*.8, y, type, alpha=alpha, ha='right', va='center', color='grey', fontdict=dict(fontsize=font_size)) # Type
        # Coverage, freq, uniq_ratio
        for j, v in enumerate(fts):
            _wedge = mpatches.Wedge((x+x_unit+table_unit*(.85 + 0.05*j), y), 0.2, 0, 360*v, alpha=alpha, width=0.12, ec="none", fc=colors[j])
            ax.add_artist(_wedge)
    
    
    def split_title_line(title_text, max_words=24):  # , max_words=None):
        """
        A function that splits any string based on specific character
        (returning it with the string), with maximum number of words on it
        """
        splited = []
        text = title_text[:]
        while text != '':
            splited.append(text[:max_words])
            text = text[max_words:]
        return '\n'.join(splited)
    
    
    def base_set(x_base, y_base, fi, max_words):
        f = flist[fi]
        table_name = "/".join(f.split('/')[-1].split(sep)[2:])[:-4]
        table_name = split_title_line(table_name, max_words=max_words)
        
        # if (table_name == 'EX') | (fi == 1):
        #     x_base += table_unit*1.4 * (fi//max_num_table)
        #     y_base = 0 
        # elif fi != 0:
        #     y_base -= i+5

        if fi == 1:
            x_base = table_unit*1.4
            y_base = 0
        elif fi > 1:
            y_base -= i + 5
        if y_base < -max_num_row:
            y_base = 0
            x_base += table_unit*1.4
        
        plt.text(x_base+table_unit*.42, y_base+1, f'{y_base}'+' '+table_name, ha='center', fontdict=dict(fontsize=font_size*1.4))
        for j, v in enumerate(features):
            plt.text(x_base+x_unit+table_unit*(.85 + 0.05*j), y_base+1, v[:3], ha='center', fontdict=dict(fontsize=font_size*.8))
        return x_base, y_base
    
    
    def set_bbox():
        box = mpatches.FancyBboxPatch((x_base-x_unit*.5, y_base-i-.3), table_unit*1.08, i+2.3, ec="none", fc='grey', alpha=.12,
                                boxstyle=mpatches.BoxStyle("Round", pad=.4))
        ax.add_artist(box)
    
    
    N_ROW = 1
    N_COL = 1
    plt.rcParams['font.family'] = ['NanumSquare', 'Helvetica']
    plt.rcParams['svg.fonttype'] = svg_fonttype
    
    fig=plt.figure(figsize = (X_SIZE*N_COL,Y_SIZE*N_ROW), dpi=DPI)
    spec = gridspec.GridSpec(ncols=N_COL, nrows=N_ROW, figure=fig, )#, width_ratios=[1,1,.1], wspace=.3)
    axes = []
    axi=0
    ax = fig.add_subplot(spec[axi//N_COL,axi%N_COL]) # row, col
    
    ########### Draw ###########
    x_base, y_base = 0, 0
    patches = []
    for fi, df_desc in enumerate(df_descs):
        x_base, y_base = base_set(x_base, y_base, fi, max_words=30)
        for i, idx in enumerate(df_desc.index):
            write_item() ## a line
        set_bbox()
    
    plt.xlim([-x_unit*2, table_unit*4])
    
    # plt.tight_layout()
    plt.axis('off')
    plt.title(title, y=0.985, fontdict=dict(fontsize=font_size*2))
    
    return fig