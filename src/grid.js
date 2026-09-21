/**
 * 可配置空间网格：把稳定区域编码映射到归并单元。
 * 配置了 defaultCellId 时，未映射区域落入默认单元；否则上报会被拒绝。
 */
export class Grid {
  constructor({ cells, defaultCellId = null }) {
    if (!Array.isArray(cells) || cells.length === 0) {
      throw new Error("网格配置至少需要一个单元");
    }
    this.regionToCell = new Map();
    this.cellIds = new Set();
    for (const cell of cells) {
      if (!cell || typeof cell.cellId !== "string" || !Array.isArray(cell.regions)) {
        throw new Error("网格单元必须包含 cellId 与 regions 数组");
      }
      this.cellIds.add(cell.cellId);
      for (const region of cell.regions) {
        if (this.regionToCell.has(region)) {
          throw new Error(`区域 ${region} 被重复配置到多个网格单元`);
        }
        this.regionToCell.set(region, cell.cellId);
      }
    }
    if (defaultCellId !== null && !this.cellIds.has(defaultCellId)) {
      throw new Error(`默认单元 ${defaultCellId} 不在网格配置中`);
    }
    this.defaultCellId = defaultCellId;
  }

  /** 返回区域所属单元；未映射且无默认单元时返回 null。 */
  cellForRegion(region) {
    const cellId = this.regionToCell.get(region);
    if (cellId) return cellId;
    return this.defaultCellId;
  }

  has(cellId) {
    return this.cellIds.has(cellId);
  }
}
