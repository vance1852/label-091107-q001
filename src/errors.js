/** 业务错误：路由层据此映射 HTTP 状态码与稳定错误码。 */
export class ServiceError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = "ServiceError";
    this.status = status;
    this.code = code;
  }
}
